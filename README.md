# ghgc-docs
This repository contains documentation for the [US Greenhouse Gas Center](https://earth.gov/ghgcenter). The documentation is rendered and published using [Quarto](https://quarto.org/).

# Structure
- `cog_transformation/`: Contains transformation scripts used to convert raw datasets into Cloud Optimized GeoTIFFs (COGs) for the US GHG Center.
- `generating_statistics_for_validation`/: Includes notebooks and scripts for generating data statistics of both raw and transformed files.
- `processing_and_verification_reports/`: Houses verification reports generated to validate the data transformations.
- `user_data_notebooks/`: Provides notebooks that guide users on how to access and utilize datasets within the US GHG Center.
- `data_workflow/`: Contains a detailed data flow diagram illustrating the entire dataset processing workflow.

# Setup
Follow the following steps to run this project locally on your machine.

## Clone the repository
```sh
git clone git@github.com:US-GHG-Center/ghgc-docs.git
cd ghgc-docs
```

## Requirements
- Follow the instructions at [Quarto Installation Guide](https://quarto.org/docs/get-started/) to install Quarto on your machine.
- `quarto install tinytex`
- `pip install jupyter`

## Preview
To preview your project locally on `localhost`, use the following command:
```
quarto preview
```
To render each `.qmd` file as `html` in the `_site` folder, run:
```
quarto render --to html
```

# Contributing
To contribute to this repository:
1. Clone this repository.
2. Branch out from staging to your feature branch.
3. Push your changes to the feature branch.
4. Create a Pull Request (PR) to staging for review.