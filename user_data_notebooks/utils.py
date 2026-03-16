import numpy as np
import os
import pandas as pd
import xarray as xr
import datetime


def sum_total_emissions(emissions, areas, mask):
    """
    Function to sum total emissions across the region of interest.

    Arguments:
        emissions : xarray data array for emissions across inversion domain
        areas     : xarray data array for grid-cell areas across inversion domain
        mask      : xarray data array binary mask for the region of interest

    Returns:
        Total emissions in Tg/y
    """

    s_per_d = 86400
    d_per_y = 365
    tg_per_kg = 1e-9
    emissions_in_kg_per_s = emissions * areas * mask
    total = emissions_in_kg_per_s.sum() * s_per_d * d_per_y * tg_per_kg
    return float(total)


def get_posterior_emissions(prior, scale):
    """
    Function to calculate the posterior emissions from the prior 
    and the scale factors. Properly accounting for no optimization 
    of the soil sink.
    Args:
        prior  : xarray dataset
            prior emissions
        scales : xarray dataset scale factors
    Returns:
        posterior : xarray dataset
            posterior emissions
    """
    # keep attributes of data even when arithmetic operations applied
    xr.set_options(keep_attrs=True)
    
    # we do not optimize soil absorbtion in the inversion. This 
    # means that we need to keep the soil sink constant and properly 
    # account for it in the posterior emissions calculation.
    # To do this, we:
    
    # make a copy of the original soil sink
    prior_soil_sink = prior["EmisCH4_SoilAbsorb"].copy()
    
    # remove the soil sink from the prior total before applying scale factors
    prior["EmisCH4_Total"] = prior["EmisCH4_Total"] - prior_soil_sink
    
    # scale the prior emissions for all sectors using the scale factors
    posterior = prior.copy()
    for ds_var in list(prior.keys()):
        if "EmisCH4" in ds_var:
            posterior[ds_var] = prior[ds_var] * scale["ScaleFactor"]
    
    # But reset the soil sink to the original value
    posterior["EmisCH4_SoilAbsorb"] = prior_soil_sink
    
    # Add the original soil sink back to the total emissions
    prior["EmisCH4_Total"] = prior["EmisCH4_Total"] + prior_soil_sink
    posterior["EmisCH4_Total"] = posterior["EmisCH4_Total"] + prior_soil_sink
    
    return posterior


def moving_average(x, w):
    """ 
    Function to compute the moving average of a time series of emission estimates
    
    Arguments
        x : time series as numpy array
        w : width of moving average window

    Returns
        mv_avg : moving average representation of x
    """
    mv_avg = np.convolve(x, np.ones(w), 'same') / w
    w2 = int(w/2)
    mv_avg[:w2] = np.mean(x[:w2])
    mv_avg[-w2:] = np.mean(x[-w2:])
    return mv_avg


def gather_estimates(n_periods, last_ROI_element, days_per_period):
    """
    Function to gather the time series of prior and posterior emission estimates from IMI output files.
    """

    # Load
    periods = pd.read_csv("periods.csv")
    statevector = xr.load_dataset("StateVector.nc")["StateVector"]
    mask = statevector <= last_ROI_element
    
    # Get estimates
    prior_emissions = [] # TODO populate this and get average from weekly hemco?
    kf_prior_emissions = []
    posterior_emissions = []

    for p in range(1, n_periods+1):
        # start date
        start = periods.loc[periods["period_number"] == p]["Starts"].values[0]
        # hemco
        hemco_file = os.path.join("hemco_emissions", f"HEMCO_diagnostics.{start}0000.nc")
        hemco_diags = xr.load_dataset(hemco_file)
        gridcell_areas = hemco_diags["AREA"]
        hemco_prior = hemco_diags.copy(deep=True)
        # scale factors (prior & posterior)
        sf_prio = xr.load_dataset(os.path.join("scale_factors", f"prior_sf_period{p}.nc"))
        sf_post = xr.load_dataset(os.path.join("scale_factors", f"posterior_sf_period{p}.nc"))
        # multiply and remove soil sink from total
        prio_emis = get_posterior_emissions(hemco_diags, sf_prio) # misnomer but it works
        post_emis = get_posterior_emissions(hemco_diags, sf_post)
        prio_emis = prio_emis["EmisCH4_Total"].isel(time=0, drop=True) - prio_emis["EmisCH4_SoilAbsorb"]
        post_emis = post_emis["EmisCH4_Total"].isel(time=0, drop=True) - post_emis["EmisCH4_SoilAbsorb"]
        hemc_emis = hemco_prior["EmisCH4_Total"] - hemco_prior["EmisCH4_SoilAbsorb"]
        # sum
        total_hemc_emis = sum_total_emissions(hemc_emis, gridcell_areas, mask)
        total_prio_emis = sum_total_emissions(prio_emis, gridcell_areas, mask)
        total_post_emis = sum_total_emissions(post_emis, gridcell_areas, mask)
        # append
        prior_emissions.append(total_hemc_emis)
        kf_prior_emissions.append(total_prio_emis)
        posterior_emissions.append(total_post_emis)

    kf_prior_emissions = np.array(kf_prior_emissions)
    posterior_emissions = np.array(posterior_emissions)
    
    # Get dates
    dt = []
    for p in range(1,n_periods+1):
        start_day = periods.loc[periods["period_number"] == p]["Starts"].values[0]
        start_dt = datetime.datetime.strptime(str(start_day),'%Y%m%d')
        middle_dt = start_dt + datetime.timedelta(hours=24*days_per_period/2)
        dt.append(middle_dt)
        
    return dt, prior_emissions, kf_prior_emissions, posterior_emissions, periods


def get_annual_emissions(dt, emis, years):

    df = pd.DataFrame()
    df["dt"] = dt
    df["year"] = [r.year for r in dt]
    df["emis"] = emis
    median_emis = []
    mean_emis = []
    std_emis = []
    emis = dict()
    for y in years:
        df_y = df[df.year == y]
        median_emis.append(np.median(df_y.emis.values))
        mean_emis.append(np.mean(df_y.emis.values))
        std_emis.append(np.std(df_y.emis.values))
        emis[y] = df_y.emis.values

    return emis, mean_emis, std_emis, median_emis