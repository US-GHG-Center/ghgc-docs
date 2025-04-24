
from pylab import * 
import xarray as xr 
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from scipy import stats
import sys

def get_country_geometry(countryname = "United States"):
    """
    Get the geometry of a country using the cartopy library

    """
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.io.shapereader as shpreader
    # Create a map in PlateCarree projection

    # Get the shape of USA
    shapename = 'admin_0_countries'
    countries_shp = shpreader.natural_earth(resolution='110m', category='cultural', name=shapename)
    for country in shpreader.Reader(countries_shp).records():
#        if country.attributes['NAME_LONG'] == 'United States':
        if country.attributes['NAME_LONG'] == countryname:            
            geometry = country.geometry
            return geometry
        else:
            print(f"Country {countryname} not found in the shapefile.")



def giveGOSATdata(data_folder_path, analysis_year = 2015, TestFiles = False):
    """
    Load GOSAT data from the netcdf file. 
    """
    print ("Loading GOSAT Data")
    data_path=  data_folder_path + f"/GOSAT/Emissions_USA_{analysis_year}.nc" 

    #data_path = f"{data_path}Emissions_USA_{analysis_year}.nc"
    ds = xr.open_dataset(data_path)

    # Process GOSAT inversion data
    lats = arange (90-0.5, -90, -1)
    lons = arange (-180+0.5, 180, 1) 
    lat_grid , lon_grid= meshgrid(lats, lons)
    lat_grid_flat = lat_grid.T.flatten()[::-1]
    lon_grid_flat = lon_grid.T.flatten()

    emis_category = ["live", "waste", "rice", "coal" , "oil", "gas" ] 
    #emis_category_names = ["livestock" , "waste", "rice", "coal" , "oil", "gas" ]
    gosat_dict = {}
    lon_list = [] ;lat_list = [] ; cat_list= []
    for icat, cat in enumerate(emis_category):
        dcat = cat 
        fcat = cat  

        if cat == "live":
            dcat = "livestock"
        gosat_dict[dcat] = {} 
        st_index = ds[f"index_{fcat}_start"].values[0]
        gosat_dict[dcat]["lon"] = array(lon_grid_flat[ds[f"lonlat_{fcat}"].values] )
        gosat_dict[dcat]["lat"] = array(lat_grid_flat[ds[f"lonlat_{fcat}"].values])
        lon_list = lon_list + list(gosat_dict[dcat]["lon"]) 
        lat_list = lat_list + list(gosat_dict[dcat]["lat"]) 
        cat_list = cat_list + [dcat] * len(gosat_dict[dcat]["lon"])
        emiss_values = ds.x_a[st_index:st_index + len(gosat_dict[dcat]["lon"])].values * 1000
    dummy_x= zeros_like(len(lon_list))
    emis_category = ["livestock", "waste", "rice", "coal" , "oil", "gas"  ] 
    # Small test to check the orentation of the AK matrix
    if TestFiles:
        x_test1= ds.x_a + np.matmul (ds.ak.values, ds.x_est.values-ds.x_a.values  ) 
        plot(x_test1 - ds.x_test) # these values should be close to zero

    # add all data to analysis dict 
    vd_cats_index = array(cat_list) != "fire"  # remove the fire category. they have negligible emissions
    ana_dict = {} # analysis dict 
    ana_dict["lon"] =  array(lon_list)[vd_cats_index] 
    ana_dict["lat"] =  array(lat_list)[vd_cats_index] 
    ana_dict["cat"] =  array(cat_list)[vd_cats_index] 
    ana_dict["x_est"] =  ds.x_est [vd_cats_index] 
    ana_dict["x_a"] =  ds.x_a.values[vd_cats_index] 
    ana_dict["x_test"] =  ds.x_test.values[vd_cats_index] 
    ana_dict["S_a"] =  ds.sa.values[vd_cats_index, :] [:,vd_cats_index] 
    ana_dict["S_x"] =  ds.sx.values[vd_cats_index, :] [:,vd_cats_index] 
    ana_dict["AK"] =  ds.ak.values[vd_cats_index, :] [:,vd_cats_index] 
    ana_dict["epa_2018"] =  zeros_like(ana_dict["x_est"]) 
    
    for key in ana_dict.keys():     ana_dict[key] = array(ana_dict[key]) 
#    ana_dict["ID_pos"] =  (((ana_dict["lon"]*10) *1000  + (ana_dict["lat"]*10).astype(int) ).astype(int)

    return ana_dict


# read EPA data 
def getEPADict(data_path, analysis_year = 2015):
    """
    Load EPA data from the netcdf file.
    """
    print ("Loading EPA Data")
    import xarray as xr
    epa_year = analysis_year
    epa_data_fid = xr.open_dataset(data_path)
    epa_data_fid
    Avaganadro_number = 6.02214076e23
    methaneM = 16.04
    grid_cell_area = epa_data_fid["grid_cell_area"][0] 
    cf_2_kghrcell= grid_cell_area *methaneM* 3600   / Avaganadro_number / 1000 
    cf_2_tgyrcell = cf_2_kghrcell * 365.25 * 24*  1e-9   
    temp_emis = zeros_like(grid_cell_area)
    emis_categories = ["livestock", "waste", "rice", "coal", "oil", "gas"]
    epa_subcategories_dict = {
        "emi_ch4_1A_Combustion_Mobile": "other" , 
        "emi_ch4_1A_Combustion_Stationary": "other",
        "emi_ch4_1B1a_Abandoned_Coal" : "coal",
        "emi_ch4_1B1a_Surface_Coal": "coal",
        "emi_ch4_1B1a_Underground_Coal": "coal",
        "emi_ch4_1B2a_Petroleum_Systems_Exploration" : "oil",
        "emi_ch4_1B2a_Petroleum_Systems_Production": "oil",
        "emi_ch4_1B2a_Petroleum_Systems_Refining": "oil",
        "emi_ch4_1B2a_Petroleum_Systems_Transport": "oil",
        "emi_ch4_1B2ab_Abandoned_Oil_Gas": "other",
        "emi_ch4_1B2b_Natural_Gas_Distribution": "gas",
        "emi_ch4_1B2b_Natural_Gas_Exploration": "gas",
        "emi_ch4_1B2b_Natural_Gas_Processing":  "gas", 
        "emi_ch4_1B2b_Natural_Gas_Production": "gas",
        "emi_ch4_1B2b_Natural_Gas_TransmissionStorage": "gas",
        "emi_ch4_2B8_Industry_Petrochemical" : "other",
        "emi_ch4_2C2_Industry_Ferroalloy": "other",
        "emi_ch4_3A_Enteric_Fermentation": "livestock",
        "emi_ch4_3B_Manure_Management": "livestock",
        "emi_ch4_3C_Rice_Cultivation": "rice",
        "emi_ch4_3F_Field_Burning": "fire",
        "emi_ch4_5A1_Landfills_Industrial" : "waste",
        "emi_ch4_5A1_Landfills_MSW": "waste",
        "emi_ch4_5B1_Composting": "waste",
        "emi_ch4_5D_Wastewater_Treatment_Domestic": "waste",
        "emi_ch4_5D_Wastewater_Treatment_Industrial": "waste" } 

    def reverse_dict(input_dict):
        reversed_dict = {}
        for subcategory, category in input_dict.items():
            if category in reversed_dict:
                reversed_dict[category].append(subcategory)
            else:
                reversed_dict[category] = [subcategory]
        return reversed_dict
    epa_subcategories_dict = reverse_dict(epa_subcategories_dict)
    epa_dict = {} 

    for cat, sub_cats in epa_subcategories_dict.items():
            temp_emis = zeros_like(temp_emis)
            for sub_cat in sub_cats:
                    temp_emis = temp_emis + epa_data_fid[sub_cat][0] * cf_2_tgyrcell 
            epa_dict[cat] = temp_emis.values
    epa_dict["lon"] = epa_data_fid.lon.values
    epa_dict["lat"] = epa_data_fid.lat.values
    emis_category_total = zeros_like(epa_dict["oil"]) 
    for emis_cat in emis_categories + ["other"]: 
        emis_category_total = emis_category_total + epa_dict[emis_cat]
    epa_dict["total"] = emis_category_total
    return epa_dict



# error paramenter from Massaakers et al., (2023 )
def getEPAErrorParams(epa_dict):
    """
    Read and calculate the error parameters for the EPA data.
    """
    print ("Loading EPA Error Parameters")
    epa_error_params = {
        'livestock': [15.5, 88, 3.12],
        'rice': [46.5, 88, 3.12], # using livestock values for 0.1 degree error and error decay coefficient
        'waste': [ (25.5 + 35 )/2 , (19 + 32)/2 , (4.02 + 10.82)/2 ], # using average of Wastewater treatment and landfills

        'gas': [14.5, 44, 0.13], 
        'oil': [32.5, 38, 0.71],  
        "coal" : [14.5, 19 , 4.02] } 
    for key in epa_error_params.keys(): epa_error_params[key] = array(epa_error_params[key])

    epa_error_params["other"] = epa_error_params["waste"]

    tot = 0 
#    print(f"{'Category':.10s}  : {'Emissions'}")

    error_temp = []
    for cat in epa_error_params.keys()- ["total"]: 
        cat_emissions = sum(epa_dict[cat])
#        print(f"{cat:10s}: {cat_emissions:.2f} Tg/yr")
        error_temp.append(cat_emissions * epa_error_params[cat][0] / 100)
        tot += cat_emissions
#    print(f"{'Total':.10s}     : {tot:.2f} Tg/yr")
    epa_error_params["total"] = [ sqrt(sum(array(error_temp)**2)), 0 ]/ tot * 100
    epa_error_params["total"]
    return epa_error_params


# clusterring of EPA data to GOSAT Grid Resolution
def clusterEPA2GOSAT(ana_dict, epa_dict, epa_error_params):
    """ 
    Cluster EPA data to GOSAT Grid Resolution
    """
    print ("Clustering EPA data to GOSAT Grid Resolution")
    gosat_res = 1
    epa_res = 0.1

    ana_dict["x_epa"] = zeros_like(ana_dict["x_est"]) * np.nan
    ana_dict["error_S_epa"] = zeros_like(ana_dict["x_est"]) * np.nan 

    skip_latlon= []
    for ilat, lat in enumerate(ana_dict["lat"]):
                lon = ana_dict["lon"][ilat]
                cat = ana_dict["cat"][ilat]
                lat_vd_index = where( (epa_dict["lat"] > lat - gosat_res/2) & (epa_dict["lat"] < lat + gosat_res/2))[0]
                lon_vd_index = where ((epa_dict["lon"] > lon - gosat_res/2) & (epa_dict["lon"] < lon + gosat_res/2) )[0]
                if len(lat_vd_index) < 10 or len(lon_vd_index) < 10:
                    skip_latlon.append([lat, lon])
                    continue

                ana_dict["x_epa"][ilat] = nansum(epa_dict[cat][lat_vd_index,:][:,lon_vd_index])
                percent_error =  exp( -  epa_error_params[cat][-1]*  (gosat_res- epa_res ) ) * epa_error_params[cat][1] + epa_error_params[cat][0]  
                ana_dict["error_S_epa"][ilat] = ana_dict["x_epa"][ilat]  * percent_error / 100


    valid_epa_index = ~isnan(ana_dict["x_epa"])
    for key in ana_dict.keys():
        if len(ana_dict[key].shape) == 2:
            ana_dict[key] = array(ana_dict[key])[valid_epa_index,:] [:,valid_epa_index] 
        else:
            ana_dict[key] = array(ana_dict[key])[valid_epa_index] 
    for key in ["S_epa_S_diff", "x_test", "epa_2018"]: 
        if key in ana_dict.keys():
            del ana_dict[key] # delete some dummy variable keys from the dictionary. 

    return ana_dict




# Make category wise regression plots for pixels with not zero emissions 
def plotRegression(xx, yy, ax , xlabel = None, 
                   ylabel = None, title = "Measured vs Predicted"):
    """
    Plot regression between two variables.
    """

    slope, intercept, r_value, p_value, std_err = stats.linregress(xx, yy)
    vd_index = np.logical_and(xx >  0.0001, yy > 0.0001)  

    xx= xx[vd_index] 
    yy= yy[vd_index]
    ax.plot(xx, yy, '.', label='data', color='red')
    ax.plot(xx, intercept + slope*xx, 'r', label='fitted line', color='red', lw = 0.5 )
    ax.plot(xx, xx, label='true', ls = "-.",  color='black', lw = 0.9 )
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title.title())
    ax.grid(True, linestyle='--', linewidth=0.5)
    txt = f"R = {r_value:.2f}\nSlope = {slope:.2f} \n N = {len(xx)}" 
    ax.text(0.05, 0.95, txt, transform=ax.transAxes, fontsize=9, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))


def plotCategoryWiseRegressions(xparam, yparam, emission_cats, ana_dict, analysis_year = 2015):
    """
    Plot category wise regressions for the given x and y parameters.
    """
    print ("Plotting Category Wise Regressions")
    fig , axs = subplots(2,3, figsize=(7,5),dpi = 200)
    axs= axs.flatten()
    for icat , cat in enumerate(emission_cats):
        ax = axs[icat]
        cat_vd_index = where(ana_dict["cat"] == cat)[0] 

        plotRegression(ana_dict[xparam][cat_vd_index], ana_dict[yparam][cat_vd_index], ax, title = cat)
    #    fig.savefig(f"plots/epa_vs_gosat_{cat}.png", bbox_inches = "tight", dpi = 200)
    #    fig.savefig(f"plots/epa_vs_gosat_{cat}.pdf", bbox_inches = "tight", dpi = 200)
    unit = "\n (Tg CH$_4$ yr$^{-1}$)"    
    if yparam=="x_epa": ylabell =   "EPA Inventory"
    if yparam=="x_est": ylabell =  "GOSAT"

    if xparam=="x_epa": xlabell =   "EPA Inventory"
    if xparam=="x_est": xlabell =  "GOSAT"
    
    if xparam == "x_epa_est": xlabell =  "EPA with GOSAT AK" 
    if yparam == "x_epa_est": ylabell =  "EPA with GOSAT AK"
    axs[0].set_ylabel(ylabell + unit) 
    axs[3].set_ylabel(ylabell + unit)
    axs[5].set_xlabel(xlabell+ unit)
    axs[4].set_xlabel(xlabell+ unit )
    axs[3].set_xlabel(xlabell+ unit)
    fig.suptitle(f"{xlabell} vs {ylabell} for {analysis_year} USA emissions")
    fig.tight_layout()

    return fig, axs



# make small annual files from hourly-month FOG files
def makeAnnualFOGFiles(year = 2018, source_path = "", destination_path = ""):
    source_path= "/Users/pandeysu/Desktop/data_raw/inv_COMP_GHGC_250113/FOG/" # change the source path to the location of the FOG files
    destination_path= "/Users/pandeysu/Desktop/data_raw/inv_COMP_GHGC_250113/GOSAT_inventroy_evaluation_data/FOG/" # change the destination path to the location where the annual files will be saved.

    print ("Making Annual FOG emissions data from monthly files")

    import calendar


    moletomassCH4 = 16.04
    #moletomassCH4 * 1.0627129e+08 * 24* 365 /1e12
    fog2TgHrCF=  moletomassCH4 * 24 / 1e12
    fold =  source_path 
    fog_cube= zeros((1008, 1332))


    for month in np.arange(1, 13):
        _, num_days = calendar.monthrange(year, month)
        fog01_12= xr.open_dataset(fold + "%s/Month%2.2i/FOG_data_%s_Month%2.2i_00to12Z.nc"%(year, month, year, month))["HC01"]
        fog12_24 = xr.open_dataset(fold + "%s/Month%2.2i/FOG_data_%s_Month%2.2i_12to24Z.nc"%(year, month, year, month))["HC01"]
        if not 'xlon' in globals():
            xlat = fog01_12["XLAT"].values
            xlon = fog01_12["XLONG"].values

        fogemis= (np.mean(fog01_12, axis = 0).values + np.mean(fog12_24, axis = 0).values) /2 * fog2TgHrCF
        fog_cube = fog_cube + fogemis * num_days

#        monthly_sum = np.sum(fogemis * num_days ) 
#        print (f"Month {month} : {monthly_sum:.5f} Tg CH4/yr")

        fog01_12.close() 
        fog12_24.close()

    fog_dic = xr.Dataset({
        "emiss": (["x", "y"], fog_cube),
        "lat": (["x", "y"], xlat),
        "lon": (["x", "y"], xlon)
    })
    print (sum(fog_cube))
    fog_dic['emiss'].attrs['units'] = 'TgCH4 yr per cell'

    fog_dic.to_netcdf(destination_path + "fog_emissions_ch4_%s.nc"%year)  
    fog_dic.close()
    return fold + "fog_emissions_ch4_%s.nc"%year



def clusterFOG2GOSAT(data_path, ana_dict_fog, analysis_year):
    print ("Clustering FOG data to GOSAT Grid Resolution")    
    year = analysis_year
    fog_dat= xr.open_dataset(data_path + "/FOG/fog_emissions_ch4_%s.nc"%year) 

  # clusterring of FOG data to GOSAT Grid Resolution
    fog_dict= {}
    for kk in fog_dat.keys():        fog_dict[kk]= fog_dat[kk].values.flatten()
    gosat_res = 1
    ana_dict_fog["x_fog"] = zeros_like(ana_dict_fog["x_est"]) * np.nan
    ana_dict_fog["error_S_fog"] = zeros_like(ana_dict_fog["x_est"]) * np.nan 
    skip_latlon= []
    for ilat, lat in enumerate(ana_dict_fog["lat"]):
                lon = ana_dict_fog["lon"][ilat]
                lat_vd_index = np.logical_and( (fog_dict["lat"] > lat - gosat_res/2) , (fog_dict["lat"] <= lat + gosat_res/2))
                lon_vd_index = np.logical_and ((fog_dict["lon"] > lon - gosat_res/2) , (fog_dict["lon"] <= lon + gosat_res/2) )
                vd_index = np.logical_and(lat_vd_index, lon_vd_index)
                if sum(vd_index) < 10 :
                    skip_latlon.append([lat, lon])
                    continue
                cat= ana_dict_fog["cat"][ilat]                      
    #            print (ilat, lat_vd_index[0], lon_vd_index[0], cat , nansum(fog_dict["emiss"][vd_index]))
                ana_dict_fog["x_fog"][ilat] = nansum(fog_dict["emiss"][vd_index])
                percent_error =  30 
                ana_dict_fog["error_S_fog"][ilat] = ana_dict_fog["x_fog"][ilat]  * percent_error / 100


    valid_fog_index = ~isnan(ana_dict_fog["x_fog"])
    for key in ana_dict_fog.keys():

        if len(ana_dict_fog[key].shape) == 2:
            ana_dict_fog[key] = ana_dict_fog[key][valid_fog_index,:][:,valid_fog_index]
        else:
            ana_dict_fog[key] = ana_dict_fog[key][valid_fog_index] 
    return ana_dict_fog


def createFossilCategory(ana_dict):  
    print ("Creating Oil/Gas(= oil + gas) production Category")
    lat_lon = []
    for  ilon, llon in enumerate(ana_dict["lon"]):
        llat = ana_dict["lat"][ilon]
        lat_lon.append(str(llon) + "_" + str(llat) )
    ana_dict["lat_lon"] = array(lat_lon )

    vd= np.logical_or(ana_dict["cat"] == "oil", ana_dict["cat"] == "gas" )

    ana_dict_fog = {"lat" : [], "lon": [], "cat": [], 
                    "x_est": [], "x_a": [], "x_epa": [], "x_epa_est": [] ,  "x_edgar":[] 
 }
    indexes = []
    for iim , lat_lon in enumerate(unique(ana_dict["lat_lon"][vd])):
        inds= where( ana_dict["lat_lon"][vd] == lat_lon)[0] 
        ana_dict_fog["lat"].append (ana_dict["lat"][vd][inds][0])
        ana_dict_fog["lon"].append (ana_dict["lon"][vd][inds][0])
        ana_dict_fog["cat"].append ("fossil")
        ana_dict_fog["x_est"].append (sum(ana_dict["x_est"][vd][inds]))
        ana_dict_fog["x_a"].append (sum(ana_dict["x_a"][vd][inds]))
        ana_dict_fog["x_epa"].append (sum(ana_dict["x_epa"][vd][inds]))
        ana_dict_fog["x_edgar"].append (sum(ana_dict["x_edgar"][vd][inds]))
        ana_dict_fog["x_epa_est"].append (0)
        
        indexes.append(inds)
        max_index = max(max(sublist) if isinstance(sublist, np.ndarray) else sublist for sublist in indexes)
    size_old_vector = max_index + 1

    for kk in ana_dict_fog.keys():
        ana_dict_fog[kk] = array(ana_dict_fog[kk])
#    for kk in ["x_est", "x_a", "x_epa", "x_edgar"]:
#         print (kk , sum(ana_dict_fog[kk]))


    # Create the summation matrix
    summation_matrix = np.zeros((len(indexes), size_old_vector))

    for i, indices in enumerate(indexes):
        for index in indices:
            summation_matrix[i, index] = 1

    ana_dict_fog["S_x"] = matmul(summation_matrix, matmul(ana_dict["S_x"][vd,:][:,vd], summation_matrix.T))
    ana_dict_fog["S_a"] = matmul(summation_matrix, matmul(ana_dict["S_a"][vd,:][:,vd], summation_matrix.T))
    ana_dict_fog["S_epa"] = matmul(summation_matrix, matmul(ana_dict["S_epa"][vd,:][:,vd], summation_matrix.T))
    ana_dict_fog["S_epa_est"] = matmul(summation_matrix, matmul(ana_dict["S_epa_est"][vd,:][:,vd], summation_matrix.T))
    ana_dict_fog["AK"] = matmul(summation_matrix, matmul(ana_dict["AK"][vd,:][:,vd], summation_matrix.T))

    ana_dict_fog["S_edgar"] = matmul(summation_matrix, matmul(ana_dict["S_edgar"][vd,:][:,vd], summation_matrix.T))
    ana_dict_fog["S_edgar_est"] = matmul(summation_matrix, matmul(ana_dict["S_edgar_est"][vd,:][:,vd], summation_matrix.T))    
    ana_dict_fog["error_S_epa"] =  sqrt(ana_dict_fog["S_epa"].diagonal())
    ana_dict_fog["error_S_edgar"] = sqrt(ana_dict_fog["S_edgar"].diagonal())


    return ana_dict_fog 




def makeGeoPandasDataFrame(ana_dict, inventory= "edgar"):
#    print ("Creating GeoPandas DataFrame for making maps")
    import geopandas as gpd
    from shapely.geometry import Point, Polygon
    rows = []
    gosat_res_lon = 1
    gosat_res_lat = 1
    #sig_epa_est= sqrt(np.diag(ana_dict['S_epa_est']))
    # Iterate over the rows of ana_dict
    for i in range(len(ana_dict["lat"])):
        lat = ana_dict["lat"][i]
        lon = ana_dict["lon"][i]
        min_lon = lon - gosat_res_lon/2;    max_lon = lon + gosat_res_lon/2 ;    min_lat = lat - gosat_res_lat/2;    max_lat = lat + gosat_res_lat/2
        polygon = Polygon([(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)])
        row = {
            "cat": ana_dict["cat"][i],
            "x_est": ana_dict["x_est"][i],
            "x_a": ana_dict["x_a"][i],
    #        "x_epa_est": ana_dict["x_epa_est"][i],
            "AK": ana_dict["AK"][i,i],
    #        "x_epa": ana_dict["x_epa"][i],
            f"x_{inventory}": ana_dict[f"x_{inventory}"][i],
            f"x_{inventory}_est": ana_dict[f"x_{inventory}_est"][i],
    #        "S_x": ana_dict_fog["S_x"][i,i],
            "geometry": polygon }
        rows.append(row)
    gdf = gpd.GeoDataFrame(rows, crs=4326)
#    USA_area_fraction 
#    USA_area_fraction = 1- (gdf.intersection(get_country_geometry("Canada")).area + gdf.intersection(get_country_geometry("Mexico")).area)/gdf.area 
    USA_area_fraction = gdf.intersection(get_country_geometry()).area/gdf.area # 1 minus the fraction of a grid cell that fall within the boundaries of Canada and Mexico. These are weights  will be used to filter USA emissions from the rectangular box over USA. 

    
    # 1 minus the fraction of a grid cell that fall within the boundaries of Canada and Mexico. These are weights  will be used to filter USA emissions from the rectangular box over USA. 

    gdf["area_weights"]=  USA_area_fraction


    return gdf 

def FilterforCONUS(gdf):
    canada_geometry= get_usa_geometry() 
    #aaa = gdf.intersection(usa_geometry)
    aaa = gdf.intersection(canada_geometry)
    #vd_cell = aaa.area/ gdf.area  > 0.5
    gdf["area"]=  aaa.area/ gdf.area
    gdf= gdf[gdf["area"]> 0.1] 
    return gdf, gdf["area"]> 0.1





def makeBarPlotFossil(ana_dict,  epa_error_params, inventory = "epa",analysis_year = 2015):
#    print ("Making Bar Plot for Fossil Emissions")
    ana_dict = filterDict2CONUS(ana_dict)
    cat= "fossil"
    emission_cats = sort(unique(ana_dict["cat"]))
#    print(emission_cats)
    fig, ax = subplots(1, 1, figsize=(9, 4.5), dpi=200)
    barwidth = 0.5
    # Calculate the sum of x_est and x_epa_est for each category
    bar_heights = []
    bar_error = []
    cat_vd_index = ~isnan (ana_dict["lat"])
    sum_x_est = matmul(ana_dict["x_est"], cat_vd_index )
    error_x_est = sqrt(matmul(cat_vd_index ,matmul(ana_dict["S_x"], cat_vd_index) ))
  
    labelss = [] 
    colors = [  "gray",  "salmon", "salmon",  "lightgreen", "lightgreen" ,  "orange", "orange" ]

    hatches = [ "//", None,  "//", None,  "//", None, "//" ]
    bar_heights.append(sum_x_est) 
    bar_error.append(error_x_est)     
    labelss.append("GOSAT")
    for inventory in ["fog" , "epa" , "edgar" ]:
 
        sum_x_epa_est = matmul(ana_dict[f"x_{inventory}_est"],cat_vd_index)
        sum_x_epa =   matmul(ana_dict[f"x_{inventory}"],cat_vd_index)  
        error_x_epa_est = sqrt (matmul(cat_vd_index ,matmul(ana_dict[f"S_{inventory}_est"], cat_vd_index) ))
        error_x_epa = epa_error_params [cat][0] * sum_x_epa /100
        bar_heights.append(sum_x_epa) ; bar_heights.append(sum_x_epa_est);   
        bar_error.append(error_x_epa) ; bar_error.append(error_x_epa_est) 
        labelss.append(f"{inventory.upper()}") ;labelss.append(f"{inventory.upper()} (AK)"); 


    bar_heights = array(bar_heights).T
    bar_error = array(bar_error).T
    ax.bar(arange(len(bar_heights)) - barwidth/2 , bar_heights, yerr=bar_error, width=barwidth, label="GOSAT", color= colors , hatch = hatches) 
    ax.set_xticks(arange(len(labelss))-  barwidth/2, labelss, ha="center")
    ax.set_ylabel("Methane Emissions (TgCH$_4$ yr$^{-1}$)")  
    ax.set_title(f"Annual CONUS Oil/Gas Emissions {analysis_year}" ) 
    ax.grid(True, linestyle='--', linewidth=0.5, zorder = -1) 
    ax.set_ylim(0, 20)



def MakeMapsAK(gdf, cat= "gas", analysis_year = 2015, inventory = "edgar"):
    fig, axs= plt.subplots(2,3,figsize = (10,4.5), dpi = 200, subplot_kw={"projection":ccrs.PlateCarree()})
    def AddText(ax, cat, keyy):
        summ= sum (gdf[gdf["cat"] == cat][keyy])
        ax.text(0.2, 0.1, f"{summ:.1f} Tg/yr" , fontsize=10, ha="center", va="center", transform=ax.transAxes, color = "gray")


    legend_kwds =  { "shrink": 0.5, "pad" : 0.01} 
    if True:
        icat = 0
        gdf.plot(column="x_est", ax= axs[icat, 0 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds =legend_kwds)
        AddText(axs[icat, 0 ], cat, "x_est")
        axs[icat, 0 ].set_title("GOSAT");    

        gdf[gdf["cat"] == cat].plot(column=f"x_{inventory}", ax= axs[icat, 1 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds = legend_kwds)
        AddText(axs[icat, 1 ], cat, f"x_{inventory}")
    #    axs[icat, 1 ].set_title("FOG (AK)");  

        axs[icat, 1 ].set_title(f"{inventory.upper()}");      

        gdf["signifcant_diff"] = gdf["x_est"] - gdf[f"x_{inventory}"]
        gdf[gdf["cat"] == cat].plot(column="signifcant_diff", ax= axs[icat, 2 ], cmap="bwr", vmin=-0.1, vmax=0.1, legend=True, legend_kwds = legend_kwds)    
        AddText(axs[icat, 2 ], cat, "signifcant_diff")
        axs[icat, 2 ].set_title("Difference");   

        icat = icat + 1

        gdf[gdf["cat"] == cat].plot(column=f"x_{inventory}_est", ax= axs[icat, 1 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds = legend_kwds)
        AddText(axs[icat, 1 ], cat, f"x_{inventory}_est")

        axs[icat, 1 ].set_title(f"{inventory.upper()} (AK)");      

        gdf[gdf["cat"] == cat].plot(column=f"AK", ax= axs[icat, 0 ], cmap="YlGn", vmin=0, vmax=0.05, legend=True, legend_kwds = legend_kwds)
#        AddText(axs[icat, 0 ], cat, f"AK")

        axs[icat, 0 ].set_title(f"AK Trace [unitless]");      


        gdf["signifcant_diff"] = gdf["x_est"] - gdf[f"x_{inventory}_est"]
        gdf[gdf["cat"] == cat].plot(column="signifcant_diff", ax= axs[icat, 2 ], cmap="bwr", vmin=-0.1, vmax=0.1, legend=True, legend_kwds = legend_kwds)   
        axs[icat, 2 ].set_title("Difference") 
        AddText(axs[icat, 2 ], cat, "signifcant_diff")


    if True:
        axs= axs.flatten()
        for iax, ax in  enumerate(axs):
            ax.axis("off")
#            if iax==3: continue
            ax.set_xlim(-125, -65)  ;    ax.set_ylim(24, 50)  
            ax.add_feature(cfeature.STATES, linestyle=':', linewidth=0.5)
        unitt= "(TgCH$_4$ yr$^{-1}$)" 
        fig.suptitle(f"Annual CONUS {cat.title()} Emissions {analysis_year} " + unitt , fontsize=12, fontweight="semibold")
        fig.subplots_adjust(top=0.9, wspace=0.1, hspace=0.1)
        show()


# estimate EPA emissions using AK matrix for compariosn with GOSAT data 
def performAKcorrection(ana_dict, x_var , S_var ):
    print ("Performing GOSAT flux inversions AK Correction")
    AK= ana_dict["AK"]
    x_a = ana_dict["x_a"]
    x_epa = ana_dict[x_var]
    x_est= ana_dict["x_est"] 
    x_epa_est = x_a + np.matmul(AK , (x_epa - x_a)) 
    ana_dict[f"{x_var}_est"] = x_epa_est

    ana_dict[S_var] = diag(ana_dict[f"error_{S_var}"]**2) 
    In= np.eye(len(x_a))
    diff_AK = In- AK 
    S_obs = ana_dict["S_x"] -  np.matmul(diff_AK, np.matmul(ana_dict["S_a"], diff_AK.T)) 
#    if test:
    S_inv_est = np.matmul(AK, np.matmul(ana_dict[S_var], AK.T)) + S_obs + S_obs # assuming S_obs == S_model
#    else:
#    S_epa_est = np.matmul(AK, np.matmul(ana_dict["S_a"], AK.T)) + S_obs + S_obs
    S_diff = np.matmul(AK, np.matmul(ana_dict["S_a"], AK.T))  
    ana_dict[f"{S_var}_est"] = S_inv_est
    ana_dict[f"{S_var}_S_diff"] = S_diff   
#    print (f"Sum of emission: \n  {x_var}, {x_var}(AK), gosat_prior, GOSAT_est")
#    print (sum(x_epa).round(3), sum(x_epa_est).round(3) , sum(x_a).round(3) , sum(x_est).round(3)) 
    for key in ["S_epa_S_diff", "x_test", "epa_2018"]: 
        if key in ana_dict.keys():
            del ana_dict[key] # delete some dummy variable keys from the dictionary. 

    return ana_dict


def performAKcorrection_test(ana_dict, x_var , S_var ):
    print ("Performing GOSAT flux inversions AK Correction")
    AK= ana_dict["AK"]
    x_a = ana_dict["x_a"]
    x_epa = ana_dict[x_var]
    x_est= ana_dict["x_est"] 
#    x_epa_est = x_a + np.matmul(AK , (x_epa - x_a)) 
    x_epa_est = x_est + x_epa - x_a - np.matmul(AK , (x_epa - x_a)) 

    ana_dict[f"{x_var}_est"] = x_epa_est

    ana_dict[S_var] = diag(ana_dict[f"error_{S_var}"]**2) 
    In= np.eye(len(x_a))
    diff_AK = In- AK 
    S_obs = ana_dict["S_x"] -  np.matmul(diff_AK, np.matmul(ana_dict["S_a"], diff_AK.T)) 
#    if test:
    S_inv_est = np.matmul(AK, np.matmul(ana_dict[S_var], AK.T)) + S_obs + S_obs # assuming S_obs == S_model
#    else:
#    S_epa_est = np.matmul(AK, np.matmul(ana_dict["S_a"], AK.T)) + S_obs + S_obs
    S_diff = np.matmul(AK, np.matmul(ana_dict["S_a"], AK.T))  
    ana_dict[f"{S_var}_est"] = S_inv_est
    ana_dict[f"{S_var}_S_diff"] = S_diff   
    for key in ["S_epa_S_diff", "x_test", "epa_2018"]: 
        if key in ana_dict.keys():
            del ana_dict[key] # delete some dummy variable keys from the dictionary. 

    return ana_dict


def createTag(lon, lat):
    tlon = (lon*10).astype(int)   
    tlat = (lat*10).astype(int)
    tagg= []
    for ii in arange(len(lon)):
        fff= "%4.4i_%4.4i"%(tlon[ii], tlat[ii])
        tagg.append(fff)
    return array(tagg)



def adjustLatLongFormat( lon, lat, vari= None):
    '''makes adjustment for latitude ( 90 to -90 => -90 to 90) and longitude (0 to 360 => -180 to 180). It return correspoding rearraged index values which can be used to adjust values on the latutude-longitude grid. 
         For example if X is a variable on the old latutude-longitude grid, the operation  
                       X= X[lat_ind,:][:,lon_ind]
          will give X on new latitude-longtide grid
    
    '''
    
    lat_ind= arange(len(lat))
    lon_ind= arange(len(lon))
    if lat[0]> lat[-1]:
        lat_ind=lat_ind[::-1]
        lat=lat[::-1]
    if max(lon)> 180:

        temp_lon= np.zeros_like(lon)
        temp_lon[:int(len(temp_lon)/2)] =lon[int(len(lon)/2):]-360  
        temp_lon[int(len(temp_lon)/2):] =lon[:int(len(lon)/2)]
        lon= temp_lon


        temp_ind= zeros_like(lon_ind)
        temp_ind[:int(len(lon)/2)] =lon_ind[int(len(lon)/2):]
        temp_ind[int(len(lon)/2):] =lon_ind[:int(len(lon)/2)]
        lon_ind = temp_ind 
    if vari is None:
        return lon, lat, lon_ind, lat_ind
    else:
        return lon, lat, vari[lat_ind,:][:,lon_ind] 


def readEdgarData(data_fold_path, analysis_year= 2015):
    print ("Reading Edgar emission inventory Data for", analysis_year)
#    print ("Reading Edgar emission inventory Data")
    cat_dic= {'oil':["PRO_OIL"],
          "gas": ["PRO_GAS"],
          "coal": ["PRO_COAL"],
          "livestock":["MNM", "ENF"],
          "rice" :["AGS"],
          "waste":["SWD_LDF", "WWT"] }
#          "wastewater" : ["WWT"] }
    edgar_dat= {}
    edgar_year= analysis_year
    lon, lat, area_gc = giveLonLatAreaArea(data_fold_path)
#    print (lon.shape, lat.shape, area_gc)
    for cat in cat_dic.keys():
        cat_values = cat_dic[cat]
        cat_dat= []
        for cat_value in cat_values:
#            print (analysis_year, edgar_year, cat_value)    
            emi_ch4= readEdgarFile(cat_value, edgar_year, data_fold_path).values

            emi_ch4= emi_ch4 * area_gc * 3600* 24 *365/ 1e9 # now in Tg/grid/year
            edlon , edlat, eem = adjustLatLongFormat (lon ,lat, emi_ch4)
            cat_dat.append(eem)
        edgar_dat[cat]= sum(array(cat_dat), axis = 0)        

    edgar_dictn= {}
    edgar_dat["lat"] = edlat
    edgar_dat["lon"] = edlon 
    for cat in cat_dic.keys():
        emi_ch4= edgar_dat[cat]
        dlat= 1 ; dlon= 1
        nem= zeros((int(180/dlat), int(360/dlon)))
        clat = zeros((int(180/dlat)))
        clon = zeros((int(360/dlon)))
        for ilat, lat11 in enumerate (arange(-89.5, 90, 1)) :
            for ilon, lon11 in enumerate (arange(-179.5, 180, 1)): 
                    vlat = np.logical_and(edgar_dat["lat"] > lat11 - 0.5, edgar_dat["lat"] <= lat11 + 0.5)
                    vlon = np.logical_and(edgar_dat["lon"] > lon11 - 0.5, edgar_dat["lon"] <= lon11 + 0.5)
                    nem[ilat, ilon]= sum(emi_ch4[vlat,:][:,vlon])
                    clat[ilat] = edlat[vlat].mean()
                    clon[ilon] = edlon[vlon].mean()
        edgar_dictn[cat]= nem
    edgar_dat= edgar_dictn.copy() 
#    edgar_dat["lat"] = clat
 #   edgar_dat["lon"] = clon
    for kk in edgar_dat.keys():
        edgar_dat[kk]=  edgar_dat[kk].flatten()
    return edgar_dat



def addEdgar2AnaDict(ana_dict, edgar_dat, analysis_year = 2015):
    print ("Adding Edgar data to ana_dict")
#    from inventory_comparison_helper import *
#    ana_dict = giveGOSATdata() 
#    ana_dict = FilterforCONUS(ana_dict)
    list(unique(ana_dict["cat"]))
    clon= arange(-179.5,    180, 1)
    clat= arange(-89.5, 90, 1)
    glon, glat = meshgrid(clon, clat)
    lonss = glon.flatten()
    latss = glat.flatten()
    ana_dict["x_edgar"] = zeros_like(ana_dict["x_est"]) * np.nan
    ana_dict["error_S_edgar"] = zeros_like(ana_dict["x_est"]) * np.nan
    ana_dict["lon"]= ana_dict["lon"].round(1)
#    for cat in unique(ana_dict["cat"]):
    for iii, latt in enumerate(ana_dict["lat"].round(1)):
        ied = where( (latss.round(1) == latt) & (lonss.round(1) == ana_dict["lon"][iii]) )[0]

        cat= ana_dict["cat"][iii]
        ana_dict["x_edgar"][iii] = edgar_dat[cat][ied] 

        ana_dict["error_S_edgar"][iii] = 0.3 * ana_dict["x_edgar"][iii]
    return ana_dict

def regridto54(lon,lat,em, dlat= 4, dlon= 5):
      nem= zeros((int(180/dlat), int(360/dlon)))
      for ilat in np.arange(nem.shape[0]):
            for ilon in np.arange(nem.shape[1]):
                olat= int(ilat*dlat/0.1   )
                olon = int(ilon*dlon/0.1)
                nem[ilat, ilon]= sum(em[olat:  int(olat + dlat/0.1), olon: int(olon + dlon/0.1)])
      return nem 

def regridto11Degree(lon,lat,em, dlat= 1, dlon= 1):
      nem= zeros((int(180/dlat), int(360/dlon)))
      for ilat in np.arange(nem.shape[0]):
            for ilon in np.arange(nem.shape[1]):
                olat= int(ilat*dlat/0.1   )
                olon = int(ilon*dlon/0.1)
                nem[ilat, ilon]= sum(em[olat:  int(olat + dlat/0.1), olon: int(olon + dlon/0.1)])
      return nem 

def readEdgarFile(cat,yr, data_path ):
    fname=   data_path +  "v6.0_CH4_%4i_%s.0.1x0.1.nc"%(int(yr), cat)
    fid = xr.open_dataset(fname) 
    emi_ch4= fid.emi_ch4
    return emi_ch4



def giveGridCellArea(lon,lat):

    # area of grid cell in meter square
    temp= zeros_like(lat)
#    import ipdb
#    ipdb.set_trace()
    dlat= lat[1]- lat[0]
    dlon= lon[1]- lon[0]
    #    for ilon in arange(len(lon)):
    ilon= 1
    longmax, longmin = lon[ilon] + dlon/2. ,  lon[ilon] - dlon/2
    for ilat in arange(len(lat)):
            latmax, latmin= lat[ilat] + dlat/2. , lat[ilat] - dlat/2.
            temp[ilat] = giveAreaCell(latmax, latmin, longmax, longmin) 
    param= (zeros((len(lon), len(lat)))+ temp).transpose()    
    return param * 1e6



def giveAreaCell(latmax, latmin, longmax, longmin):
    dlon=  pi/180.*(longmax- longmin )
    R_e = 6378.1 # Radius of Earth in kilometers
    areakm2=  abs(R_e * R_e * dlon * (sin(pi/180.*latmin) - sin(pi/180.*latmax)))
    return areakm2


def giveLonLatAreaArea(data_path):
    import glob
    file = glob.glob(data_path + "*.0.1x0.1.nc")[0] 
#    fname=    "v6.0_CH4_2015_TNR_Aviation_CDS.0.1x0.1.nc"

#    fid = xr.open_dataset(data_path + fname) 

    fid = xr.open_dataset(file) 
    lat= fid.lat.values
    lon= fid.lon.values
    area_gc= giveGridCellArea(lon, lat) 
    return lon,lat, area_gc
def FilterforCONUS(gdf):
#    usa_geometry= get_usa_geometry() 
    #aaa = gdf.intersection(usa_geometry)
#    aaa = gdf.intersection(usa_geometry)

    USA_area_fraction = 1- (gdf.intersection(get_usa_geometry("Canada")).area + gdf.intersection(get_usa_geometry("Mexico")).area)/gdf.area    
    gdf["area_weights"]=  USA_area_fraction

#    usa_geometry= get_usa_geometry() 
#    gdf["valid_USA_area"] = 1- (gdf.intersection(get_usa_geometry("Canada")).area + gdf.intersection(get_usa_geometry("Mexico")).area)    
#    vdd= gdf["area_weights"]> 0.01
#    vdd= ones(len(vdd), dtype=bool)

    return gdf


def filterDict(adict, vvd):
    ndicts= {}
    for key in adict.keys():

        if len(adict[key].shape)==2:
            ndicts[key] =  adict[key][vvd, :] [:,vvd] 
        elif len(adict[key].shape)==1:
            ndicts[key] =  adict[key][vvd]
        else:
            print ("Error: The shape of the dictionary is not 1 or 2", key)
    return ndicts

def filterDict2CONUS(ana_dict): 
    gdf_epa = makeGeoPandasDataFrame(ana_dict, inventory="epa")
    #gdf_epa = FilterforCONUS(gdf_epa)
    ana_dict_CONUS= filterDict(ana_dict, gdf_epa["area_weights"].values> 0.5 ).copy() # Filter the GeoPandas DataFrame for the CONUS region with grid cell area fraction > 50 %
    return ana_dict_CONUS



def makeBarPlot(ana_dict,  epa_error_params, inventory = "epa",analysis_year = 2015):
    ana_dict = filterDict2CONUS(ana_dict)
    emission_cats = sort(unique(ana_dict["cat"]))
    fig, ax = subplots(1, 1, figsize=(6, 3), dpi=200)
    barwidth = 0.3
    # Calculate the sum of x_est and x_epa_est for each category
    bar_heights = []
    bar_error = []
    for cat in emission_cats:
        cat_vd_index = (ana_dict["cat"] == cat).astype(int)
        sum_x_est = matmul(ana_dict["x_est"], cat_vd_index )
        sum_x_epa_est = matmul(ana_dict[f"x_{inventory}_est"],cat_vd_index)
        sum_x_epa =   matmul(ana_dict[f"x_{inventory}"],cat_vd_index)                                         
        error_x_est = sqrt(matmul(cat_vd_index ,matmul(ana_dict["S_x"], cat_vd_index) ))
        error_x_epa_est = sqrt (matmul(cat_vd_index ,matmul(ana_dict[f"S_{inventory}_est"], cat_vd_index) ))
        error_x_epa = epa_error_params [cat][0] * sum_x_epa /100
        bar_heights.append([sum_x_est , sum_x_epa_est, sum_x_epa ])
        bar_error.append([error_x_est, error_x_epa_est, error_x_epa])
    bar_heights = array(bar_heights).T
    bar_error = array(bar_error).T
    ax.bar(arange(len(emission_cats)) - barwidth/2 , bar_heights[0], yerr=bar_error[0], width=barwidth, label="GOSAT") 
    ax.bar(arange(len(emission_cats)) + barwidth/2, bar_heights[1], yerr=bar_error[1], width=barwidth, label=f"{inventory.upper()} (AK)", hatch = "//")            
    ax.bar(arange(len(emission_cats)) + 3*barwidth/2, bar_heights[2], yerr=bar_error[2], width=barwidth, label=f"{inventory.upper()}", hatch = "\\")
    ax.legend()
    ax.set_xlabel("Category")
    emission_cats_labels = [kk.title() for kk in emission_cats]
    ax.set_xticks(arange(len(emission_cats)), emission_cats_labels, ha="center")
    ax.set_ylabel("Emission (TgCH$_4$ yr$^{-1}$)")  
    ax.set_title(f"Annual CONUS Emissions {analysis_year}" ) 
    ax.grid(True, linestyle='--', linewidth=0.5, zorder = -1)
    ax.set_ylim(0, 15) 


def copyEDGARYearFiles(year):
    # Function to copy files from native EDGAR annual gridded v6 to Data_inventroy_evaluation folder 
    import os, shutil
    source_base = "/Users/pandeysu/Desktop/data_raw/Emissions/EDGAR/annual_gridded_v6/"
    destination_base = "/Users/pandeysu/Desktop/data_raw/inv_COMP_GHGC_250113/GOSAT_inventroy_evaluation_data/EDGAR/"

    # List all directories in source_base ending with '_nc'
    categories = [
        d[:-3] for d in os.listdir(source_base)
        if os.path.isdir(os.path.join(source_base, d)) and d.endswith('_nc')
    ]

    for category in categories:
        print (category)
        source_file = os.path.join(
            source_base,
            f"{category}_nc",
            f"v6.0_CH4_{year}_{category}.0.1x0.1.nc"
        )
        destination_dir = os.path.join(
            destination_base,
            f"EDGAR_v6_{year}_only"
        )
        os.makedirs(destination_dir, exist_ok=True)
        shutil.copy(source_file, destination_dir)



def MakeMaps(gdf, cat= "gas", analysis_year = 2015, inventory = "edgar"):
    gdf = gdf[gdf["area_weights"].values> 0.5]  # Filter the GeoPandas DataFrame for the CONUS region with grid cell area fraction > 50 %
    fig, axs= plt.subplots(2,3,figsize = (10,4.5), dpi = 200, subplot_kw={"projection":ccrs.PlateCarree()})
    def AddText(ax, cat, keyy):
        summ= sum (gdf[gdf["cat"] == cat][keyy] )
        ax.text(0.2, 0.1, f"{summ:.1f} Tg/yr" , fontsize=10, ha="center", va="center", transform=ax.transAxes, color = "gray")


    legend_kwds =  { "shrink": 0.5, "pad" : 0.01} 
    if True:
        icat = 0
        gdf[gdf["cat"] == cat].plot(column="x_est", ax= axs[icat, 0 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds =legend_kwds)
        AddText(axs[icat, 0 ], cat, "x_est")
        axs[icat, 0 ].set_title("GOSAT");    

        gdf[gdf["cat"] == cat].plot(column=f"x_{inventory}", ax= axs[icat, 1 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds = legend_kwds)
        AddText(axs[icat, 1 ], cat, f"x_{inventory}")
    #    axs[icat, 1 ].set_title("FOG (AK)");  

        axs[icat, 1 ].set_title(f"{inventory.upper()}");      

        gdf["signifcant_diff"] = gdf["x_est"] - gdf[f"x_{inventory}"]
        gdf[gdf["cat"] == cat].plot(column="signifcant_diff", ax= axs[icat, 2 ], cmap="bwr", vmin=-0.1, vmax=0.1, legend=True, legend_kwds = legend_kwds)    
        AddText(axs[icat, 2 ], cat, "signifcant_diff")
        axs[icat, 2 ].set_title("Difference");   

        icat = icat + 1

        gdf[gdf["cat"] == cat].plot(column=f"x_{inventory}_est", ax= axs[icat, 1 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds = legend_kwds)
        AddText(axs[icat, 1 ], cat, f"x_{inventory}_est")

        axs[icat, 1 ].set_title(f"{inventory.upper()} (AK)");      

        gdf["signifcant_diff"] = gdf["x_est"] - gdf[f"x_{inventory}_est"]
        gdf[gdf["cat"] == cat].plot(column="signifcant_diff", ax= axs[icat, 2 ], cmap="bwr", vmin=-0.1, vmax=0.1, legend=True, legend_kwds = legend_kwds)   
        axs[icat, 2 ].set_title("Difference") 
        AddText(axs[icat, 2 ], cat, "signifcant_diff")


    if True:
        axs= axs.flatten()
        for iax, ax in  enumerate(axs):
            ax.axis("off")
            if iax==3: continue
            ax.set_xlim(-125, -65)  ;    ax.set_ylim(24, 50)  
            ax.add_feature(cfeature.STATES, linestyle=':', linewidth=0.5)
        unitt= "(TgCH$_4$ yr$^{-1}$)" 
        fig.suptitle(f"Annual CONUS {cat.title()} Emissions {analysis_year} " + unitt , fontsize=12, fontweight="semibold")
        fig.subplots_adjust(top=0.9, wspace=0.1, hspace=0.1)
        show()



def MakeFossilMaps(gdf, analysis_year= None):
    gdf = gdf[gdf["area_weights"].values> 0.5]  # Filter the GeoPandas DataFrame for the CONUS region with grid cell area fraction > 50 %
    fig, axs= plt.subplots(2,3,figsize = (10,5), dpi = 200, subplot_kw={"projection":ccrs.PlateCarree()})
    cat= "fossil"
    legend_kwds =  { "shrink": 0.5, "pad" : 0.01} 
    def AddText(ax, cat, keyy):
        summ= sum (gdf[gdf["cat"] == cat][keyy] )
        ax.text(0.2, 0.1, f"{summ:.1f} Tg/yr" , fontsize=10, ha="center", va="center", transform=ax.transAxes, color = "gray")


    if True:
        icat = 0
        gdf[gdf["cat"] == cat].plot(column="x_est", ax= axs[icat, 0 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds =legend_kwds)
        axs[icat, 0 ].set_title("GOSAT");    
        AddText(axs[icat, 0 ], cat, "x_est")

        gdf[gdf["cat"] == cat].plot(column="x_fog", ax= axs[icat, 1 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds = legend_kwds)
        AddText(axs[icat, 1 ], cat, "x_fog")
    #    axs[icat, 1 ].set_title("FOG (AK)");  
    

        axs[icat, 1 ].set_title("FOG");      
        gdf["signifcant_diff"] = gdf["x_est"] - gdf["x_fog"]
        gdf[gdf["cat"] == cat].plot(column="signifcant_diff", ax= axs[icat, 2 ], cmap="bwr", vmin=-0.1, vmax=0.1, legend=True, legend_kwds = legend_kwds)    
        AddText(axs[icat, 2 ], cat, "signifcant_diff")
        axs[icat, 2 ].set_title("Difference");   

        icat = icat + 1
        gdf[gdf["cat"] == cat].plot(column="x_fog_est", ax= axs[icat, 1 ], cmap="YlOrRd", vmin=0, vmax=0.1, legend=True, legend_kwds = legend_kwds)
        AddText(axs[icat, 1 ], cat, "x_fog_est")


        axs[icat, 1 ].set_title("FOG (AK)");      
        gdf["signifcant_diff"] = gdf["x_est"] - gdf["x_fog_est"]
        gdf[gdf["cat"] == cat].plot(column="signifcant_diff", ax= axs[icat, 2 ], cmap="bwr", vmin=-0.1, vmax=0.1, legend=True, legend_kwds = legend_kwds)   
        AddText(axs[icat, 2 ], cat, "signifcant_diff") 
        axs[icat, 2 ].set_title("Difference"); 


    if True:
        axs= axs.flatten()
        for iax, ax in  enumerate(axs):
            ax.axis("off")
            if iax==3: continue
            ax.set_xlim(-125, -65)  ;    ax.set_ylim(24, 50)  
            ax.add_feature(cfeature.STATES, linestyle=':', linewidth=0.5)
        fig.suptitle(f"Annual Methane Emissions from CONUS {analysis_year} (Tg/yr)", fontsize=12, fontweight="semibold")
        fig.subplots_adjust(top=0.8, wspace=0.1)


def makeGeoPandasDataFrameFOG(ana_dict_fog):
#    print ("Creating GeoPandas DataFrame for making maps")
    import geopandas as gpd
    from shapely.geometry import Point, Polygon
    rows = []
    gosat_res_lon = 1
    gosat_res_lat = 1
    #sig_epa_est= sqrt(np.diag(ana_dict['S_epa_est']))
    # Iterate over the rows of ana_dict
    for i in range(len(ana_dict_fog["lat"])):
        lat = ana_dict_fog["lat"][i]
        lon = ana_dict_fog["lon"][i]
        min_lon = lon - gosat_res_lon/2;    max_lon = lon + gosat_res_lon/2 ;    min_lat = lat - gosat_res_lat/2;    max_lat = lat + gosat_res_lat/2
        polygon = Polygon([(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)])
        row = {
            "cat": ana_dict_fog["cat"][i],
            "x_est": ana_dict_fog["x_est"][i],
            "x_a": ana_dict_fog["x_a"][i],
            "x_epa_est": ana_dict_fog["x_epa_est"][i],
            "x_epa": ana_dict_fog["x_epa"][i],
            "x_edgar_est": ana_dict_fog["x_edgar_est"][i],
            "x_edgar": ana_dict_fog["x_edgar"][i],        
            "x_fog": ana_dict_fog["x_fog"][i],
            "x_fog_est": ana_dict_fog["x_fog_est"][i],
    #        "S_x": ana_dict_fog["S_x"][i,i],
            "geometry": polygon }
        rows.append(row)
    gdf = gpd.GeoDataFrame(rows, crs=4326)
    USA_area_fraction = gdf.intersection(get_country_geometry()).area/gdf.area # 1 minus the fraction of a grid cell that fall within the boundaries of Canada and Mexico. These are weights  will be used to filter USA emissions from the rectangular box over USA. 

    
    # 1 minus the fraction of a grid cell that fall within the boundaries of Canada and Mexico. These are weights  will be used to filter USA emissions from the rectangular box over USA. 

    gdf["area_weights"]=  USA_area_fraction

    return gdf 