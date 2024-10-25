# sales-analysis
This repo contains the walkthrough with Apache Spark where I am practicing skills on SQL queries and visualization for big data. Due to the difficulties of uploading the dataset from a local file to the Databricks platform (I registered for the Community Edition subsrciption), 

## Datasets
I had to manually create the data. Luckily for me, it wasn't a lot of data. Data preparation was done by assigning the right data types to each of the features/columns. New columns/features were also created by extracting the month, year and quarter from the date column. The first dataset (sales.csv) had the columns `product_id`, `customer_id`, `order_date`, `location`, `source_order`, `order_year`, `order_quarter`, `order_month`. The second dataset (menu.csv) has only three columns, `product_id`, `product_name`, `price`. `product_id` is the primary identifier that will be used to join the two dataframes.

## Questions to be answered
1. What is the total spending of each customer on our products?

This query was answered by joining the datasets on the primary identifier, grouping by `customer_id`, calculating the sum of the `price` and ordering by `customer_id`

    total_amount_spent = (sales_df.join(menu_df, "product_id").groupBy("customer_id")
                      .agg({"price": "sum"})
                      .orderBy("customer_id"))

Business Insights: Understanding the total amount spent by each customer helps to assess customer loyalty. By identifying high-spending customers, the business can target these individuals with promotions or loyalty programs. It also gives us insights into customer purchasing behavior.

2. What are the total revenue contributions of each product on the menu?

We first join the two dataframes based on `product_id`. Then group the data by `product_name` and calculate the sum of `price` for each product.

    total_amount_spent = (sales_df.join(menu_df, "product_id").groupBy("product_name")
                      .agg({"price": "sum"})
                      .orderBy("product_name"))

Business Insights: This query reveals which products drive the most sales. It will further help in understanding customer preferences, optimizing inventory, and potentially adjusting pricing or promotions for underperforming items.

3. How have monthly sales trends changed over time?

We join the two dataframes on `product_id`, group by the `order_month`, calculates the sum of the `price` for each month and finally order by the `order_month`.

    total_sales_month = (sales_df.join(menu_df, "product_id").groupBy("order_month")
                      .agg({"price": "sum"})
                      .orderBy('order_month'))

Business Insights: This KPI would measure the total sales each month. Tracking this can help understand revenue trends, identify high or low-performing months, and evaluate the impact of specific sales strategies over time.

4. What is the trend in total annual sales over the years? Are there any significant fluctuations or growth patterns?

The total sales revenue for each year is calculated by joining the sales dataframe with the menu dataframe on the `product_id`, grouping by `order_year`, and then summing the `price` for each group.

    total_sales_year = (sales_df.join(menu_df, "product_id").groupBy("order_year")
                      .agg({"price": "sum"})
                      .orderBy('order_year'))

Business Insights: Identifying annual sales trends can help understand seasonality, growth, or decline in demand. This can support strategic decisions like expanding product lines, targeting marketing efforts in peak periods, or adjusting pricing strategies.
KPI:

5. What were the total sales (in revenue) for each quarter?

After joining the two dataframes, we group by `order_quarter`, calculate the sum of `price` for each quarter and order by `order_quarter`.

    total_sales_quarter = (sales_df.join(menu_df, "product_id").groupBy("order_quarter")
                      .agg({"price": "sum"})
                      .orderBy('order_quarter'))

Business Insights: By analyzing the total quarterly sales, we can identify seasonal trends or peak periods, which may indicate when demand for certain products is higher or lower. It will also help in tracking overall revenue growth, and measure the impact of seasonal fluctuations.

6. Which products are the most popular based on sales frequency?

Join the two dataframes on `product_id`, then group by `product_id` and `product_name`. Count the `product_id` (give it the `product_count` alias) and order by `product_count` in descending order.

    product_counts = (sales_df.join(menu_df, "product_id").groupBy("product_id", "product_name")
                  .agg(count("product_id").alias("product_count"))
                  .orderBy("product_count", ascending=0)
                  .drop("product_id").limit(5))

Business Insights: This query provides the analysis of which products have the highest sales volume, highlighting customer preferences and helping inventory management.
KPI Example: Product Sales Frequency - Track the top products by sales count to identify customer demand trends and optimize stock levels based on popular items.

7. How frequently are customers visiting the restaurant?

We filter the sales dataframe by `source_order`, group by the `customer_id` and count the unique `order_date`.

    customer_visits = (sales_df.filter(sales_df.source_order=='Restaurant')
                   .groupBy('customer_id')
                   .agg(countDistinct('order_date')))

Business Insight: Customers with higher visit frequency may be more loyal or engaged. Tracking these visit patterns can inform targeted marketing efforts, such as loyalty rewards or personalized promotions for frequent customers.

8. Which country or region is generating the highest total sales?

We calculate the total sales by location by joining the dataframes on `product_id` and then summing up the `price` grouped by `location`.

    sales_by_country = (sales_df.join(menu_df, "product_id").groupBy('location').agg({'price': 'sum'}))

Business Insights: We can identify high-performing regions by analyzing the results from this query. Understanding this can inform market strategies, allocation of resources, or areas to potentially target for growth. This KPI can be further broken down into growth rate in sales by location to assess whether a region’s sales performance is improving over time.

9. What is the total revenue generated from each sales channel (or source of order)?

Join the two dataframes on the `product_id`, group by `source_order` and calculate the sum of the `price` for each `source_order`.

    sales_by_source = (sales_df.join(menu_df, "product_id").groupBy('source_order').agg({'price': 'sum'}))

Business Insights: This KPI measures the total revenue generated from each sales channel. It helps us to identify which channels are most effective in driving sales and consider strategic investments. It also helps us to direct marketing efforts to boost underperforming channels.