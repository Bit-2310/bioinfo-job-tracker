# Bioinfo Job Tracker

Initial setup for detecting ATS providers (Greenhouse, Lever, Ashby, iCIMS, Workday).

## Local setup (conda)
```bash
conda env create -f environment.yml
conda activate bioinfo-job-tracker
```

## Usage
1. Edit `data/companies.csv` with company names (single column).
   - Or generate it from the sponsorship file:
     ```bash
     python scripts/populate_companies_from_sponsorship.py --output data/companies_all.csv
     ```
   - Or curate a top-100 US bioinformatics list from public sources:
     ```bash
     python scripts/curate_top100_us_bioinformatics.py
     ```
     This uses BioPharmGuy's bioinformatics list plus BioPharmGuy's biotech list
     filtered by bioinformatics-related keywords.
     It also injects US-based Big Pharma from Wikipedia's largest biomedical
     companies by revenue list.
2. Verify/filter to bioinformatics/biotech companies using web sources:
```bash
python scripts/verify_bioinformatics_companies.py
```
   - Optional overrides:
     - `data/bioinformatics_allowlist.txt`
     - `data/bioinformatics_denylist.txt`
   - Outputs:
     - `data/companies.csv` (verified)
     - `data/companies_unverified.csv`
     - `data/biotech_reference_companies.csv`
3. Run the collector:
```bash
python scripts/company_api_collector.py
```

Outputs:
- `data/targeted_list.json`
- `data/no_api_companies.csv`

Notes:
- Greenhouse/Lever/Ashby detection uses their public job board APIs.
- For large scans, you can resume using `--progress-dir` and `--resume`.

4. Validate a random sample from `data/targeted_list.json`:
```bash
python scripts/validate_targeted_list.py --sample-size 250
```
Outputs:
- `data/targeted_list_validation.json`

## API codes
- 0 = none
- 1 = greenhouse
- 2 = lever
- 3 = ashby
- 4 = icims
- 5 = workday
