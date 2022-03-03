1. The BedSideMonitor.py is run by two copies in parallel to create raw data.
2. The hash key for bsm_agg_data is combination of device and datatypa eg., BSM_G101-SPO2 and range is timestamp
3. Hash key and range for bsm_alerts is similar to bsm_agg_data
4. The rule are configured in rules.json
5. The timestamp for bsm_agg_data is the starting timestamp
6. The timestamp for bsm_alerts is the first instance of breach
7. The certificates and BedSideMonitor.py for each device id is bundled in the <deviceid> folder