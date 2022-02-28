#skip this import in the main integration as we already have a filtered_df
import pandas as pd

#library needed for plotting
import plotly.express as px

path = '/home/td/cerebra_work/loyalty-engine/RFM/Notebooks/Data/'
customer_loyalty_df = pd.read_parquet(path+'uniqlo_metrics_customers_loyalty_customer_2021-10-13.parquet')

def find_triggers(product_loyalty_df):

    triggers_df = product_loyalty_df.copy()
    triggers_df = product_loyalty_df.groupby("variant_id").agg(
        {'loyalty_score': 'max'}) - product_loyalty_df.groupby("variant_id").agg({'loyalty_score': 'min'})
    triggers_df = triggers_df.rename(columns={'loyalty_score': 'difference'})
    triggers_df.to_parquet('triggers_df_after_difference_calc.parquet')
    triggers_df = triggers_df.sort_values(by='difference', ascending=False)
    triggers_df = triggers_df[triggers_df['difference'] > 1]
    triggers_df = triggers_df.iloc[0:10, :]
    



#=====================================================



    triggers_list = triggers_df.index.tolist()
    boolean_series = product_loyalty_df.variant_id.isin(triggers_list)
    filtered_df = product_loyalty_df[boolean_series]
    # dictionary to store slope values of the variants
    res = {}
    for i in triggers_list:
        print(i)
        fig = px.scatter(filtered_df.loc[filtered_df['variant_id'] == i], x="order_date", y="loyalty_score",
                         color="variant_name", trendline="ols",
                         title="Which variant is the best loyalty trigger within a category?"
                         , labels=dict(order_date="Purchase Date", loyalty_score="Loyalty Score "))
        results = px.get_trendline_results(fig)
        coefficients = results.iloc[0]["px_fit_results"].params
        res[i] = coefficients[1]
    positive_negative_triggers_dict = {}
    positive_negative_triggers_keys = sorted(res, key=res.get)
    for w in positive_negative_triggers_keys:
        positive_negative_triggers_dict[w] = res[w]
    # print(positive_negative_triggers_dict)
    negative_triggers = list(positive_negative_triggers_dict.items())[:3]
    positive_triggers = list(positive_negative_triggers_dict.items())[-3:]
    positive_triggers_list = list(zip(*positive_triggers))[0]
    negative_triggers_list = list(zip(*negative_triggers))[0]
    postive_boolean_series = customer_loyalty_df.variant_id.isin(positive_triggers_list)
    negative_boolean_series = customer_loyalty_df.variant_id.isin(negative_triggers_list)
    positive_triggers_df = customer_loyalty_df[postive_boolean_series]
    negative_triggers_df = customer_loyalty_df[negative_boolean_series]
    positive_triggers_df.to_parquet('positive_triggers_df.parquet')
    negative_triggers_df.to_parquet('negative_triggers_df.parquet')
    return positive_triggers_df, negative_triggers_df

top_three_positive_triggers, top_three_negative_triggers = find_triggers(customer_loyalty_df)
print(top_three_positive_triggers)
print("===============================================================")
print(top_three_negative_triggers)