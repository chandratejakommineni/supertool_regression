# supertool-regression

Predict nickel (Ni) and cobalt (Co) grades in CMC from feed and process features.

## Setup

```bash
pip install -r requirements.txt
```

## Structure

- **notebooks/** – Data prep (`cmc_ni_co_data_prep.ipynb`), full pipeline (`cmc_ni_co_full_pipeline.ipynb`), and ML model (`cmc_ni_co_ml_model.ipynb`).
- **requirements.txt** – Python dependencies (pandas, scikit-learn, statsmodels, etc.).

Run the full pipeline notebook for data prep → encoding → train/test split → Ni/Co models → evaluation.
