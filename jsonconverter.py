import csv
import jsonlines as jsonl
import pandas as pd
orders_df = pd.read_csv(
    ‘/Users/topher/Desktop/brazilian-ecommerce/olist_orders_dataset.csv’)
orderItems_df = pd.read_csv(
    ‘/Users/topher/Desktop/brazilian-ecommerce/olist_order_items_dataset.csv’)
sellers_df = pd.read_csv(
    ‘/Users/topher/Desktop/brazilian-ecommerce/olist_sellers_dataset.csv’)
customers_df = pd.read_csv(
    ‘/Users/topher/Desktop/brazilian-ecommerce/olist_customers_dataset.csv’)
orderitem_seller_joined_df = pd.merge(left=orderItems_df, right=sellers_df,
                                      how=‘left’, left_on=‘seller_id’, right_on=‘seller_id’)
uniqueOrderKeys = orders_df.order_id.unique()
output = “”
myfile = open(“myjson.json”, “w”)
for orderID in uniqueOrderKeys:
    o_df = orders_df[orders_df[‘order_id’] == orderID]
    oi_JSON = orderitem_seller_joined_df[orderitem_seller_joined_df[‘order_id’] == orderID].to_json(
        orient=‘records’)
    c_id = o_df.iloc[0][“customer_id”]
    c_JSON = customers_df[customers_df[‘customer_id’]
                          == c_id].to_json(orient=‘records’)
    jsonobj = ‘{“order_id”: “’ + str(orderID) + ‘“, “date”: “’ + str(
        o_df.iloc[0][‘order_purchase_timestamp’]) + ‘” , “customer”: ’ + c_JSON + ‘, “orderItem”: ’ + oi_JSON + ‘} \n’
    myfile.write(jsonobj)
myfile.close()