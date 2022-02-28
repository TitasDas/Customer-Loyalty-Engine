
import pandas as pd
import plotly.express as px
import plotly

def identify_target_segment(customer_loyalty_df, positive_triggers_df, negative_triggers_df):
	positive_trig_unique = positive_triggers_df.variant_id.unique()
	negative_trig_unique = negative_triggers_df.variant_id.unique()
	low_loyalty_boolean_series = customer_loyalty_df.loyalty_labels.isin(['Low'])
	low_loyalty_df = customer_loyalty_df[low_loyalty_boolean_series]
	negative_trigger_boolean_series = low_loyalty_df.variant_id.isin(list(negative_trig_unique))
	target_segment_df = low_loyalty_df[negative_trigger_boolean_series]
	positive_trigger_boolean_series = ~customer_loyalty_df.variant_id.isin(list(positive_trig_unique))
	non_buyers_of_positive_triggers_df = customer_loyalty_df[positive_trigger_boolean_series] 
	low_boolean = ~non_buyers_of_positive_triggers_df.loyalty_labels.isin(['Low'])
	final_non_buyers_df = non_buyers_of_positive_triggers_df[low_boolean]
	no_of_customers_low_loyalty = low_loyalty_df.customer_id.nunique()
	no_of_customers_target_segment = target_segment_df.customer_id.nunique()
	no_of_customers_non_buyers_of_positive_triggers = final_non_buyers_df.customer_id.nunique()
	target_segment_df['previous_visit'] = target_segment_df.groupby(['customer_id'])['order_date'].shift()
	target_segment_df['days_btw_visits'] = target_segment_df['order_date'] - target_segment_df['previous_visit']
	target_segment_df['days_btw_visits'] = target_segment_df['days_btw_visits'].apply(lambda x: x.days)
	avg_time = target_segment_df['days_btw_visits'].mean()
	avg_order_size = int(target_segment_df['monetary_value'].mean())
	mean_loyalty_score = int(target_segment_df['loyalty_score'].mean())
	avg_time_from_last_order = int(target_segment_df['recency'].mean())

	
	print(no_of_customers_low_loyalty, no_of_customers_target_segment, no_of_customers_non_buyers_of_positive_triggers, avg_time)

def plot_negative_section(negative_triggers_df):
	negative_triggers_df = negative_triggers_df.sort_values(by='order_date')
	fig = px.scatter(negative_triggers_df, x = "order_date", y="loyalty_score", color="variant_name", trendline="lowess", title = "What products have they bought?"
                ,labels=dict(order_date="Purchase Date", loyalty_score="Loyalty Score ", variant_name="Most commonly bought products of this segment"))
	fig.data = [t for t in fig.data if t.mode == "lines"]
	fig['data'][0]['line']['color']="#ffcf26"
	fig['data'][1]['line']['color']="#ff5454"
	fig['data'][2]['line']['color']="#f154ff"
	fig.update_traces(showlegend = True)
	#fig.update_traces(line=(dict(color="#ffcf26","ff5454","#f154ff")))
	fig.show()
	json_for_negative_triggers = plotly.io.to_json(fig)
	#print(json_for_negative_triggers)

def plot_positive_section(positive_triggers_df):
	positive_triggers_df = positive_triggers_df.sort_values(by='order_date')
	fig = px.scatter(positive_triggers_df, x = "order_date", y="loyalty_score", color="variant_name", trendline="lowess", title = "What products should you market to them?"
                ,labels=dict(order_date="Purchase Date", loyalty_score="Loyalty Score ", variant_name="Recommended products to raise customer loyalty"))
	fig.data = [t for t in fig.data if t.mode == "lines"]
	fig['data'][0]['line']['color']="#32ed72"
	fig['data'][1]['line']['color']="#54e0ff"
	fig['data'][2]['line']['color']="#a954ff"
	fig.update_traces(showlegend = True)
	#fig.update_traces(line=(dict(color="#32ed72","#54e0ff","a954ff")))
	fig.show()
	json_for_positive_triggers = plotly.io.to_json(fig)
	#print(json_for_positive_triggers)

path = '/home/td/cerebra_work/loyalty-engine/RFM/Scripts/'
customer_loyalty_df = pd.read_parquet(path+'uniqlo_metrics_customers_loyalty_customer_2021-10-13.parquet')

#positive_triggers_df = pd.read_parquet(path+'uniqlo_positive_triggers_18-10-2021.parquet')
#negative_triggers_df = pd.read_parquet(path+'uniqlo_negative_triggers_18-10-2021.parquet')
positive_triggers_df = pd.read_parquet(path+'positive_triggers_df.parquet')
negative_triggers_df = pd.read_parquet(path+'negative_triggers_df.parquet')

section_2_plot = plot_negative_section(negative_triggers_df)
section_3_plot = plot_positive_section(positive_triggers_df)

parameters = identify_target_segment(customer_loyalty_df,positive_triggers_df,negative_triggers_df)