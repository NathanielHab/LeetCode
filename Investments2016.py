import pandas as pd

def find_investments(insurance: pd.DataFrame) -> pd.DataFrame:
    # find policyholders with the same tiv_2015 value
    same_tiv_2015 = insurance[insurance.duplicated('tiv_2015', keep=False)]

    # find policyholders in unique cities
    unique_cities = insurance.drop_duplicates(subset=['lat', 'lon'], keep=False)

    # merge the two dataframes
    df = pd.merge(same_tiv_2015, unique_cities, on=['pid', 'tiv_2015', 'tiv_2016', 'lat', 'lon'])
    
    # format result
    df = df['tiv_2016'].sum()
    result = round(df, 2)
    result_df = pd.DataFrame({'tiv_2016': [result]})
    
    return result_df