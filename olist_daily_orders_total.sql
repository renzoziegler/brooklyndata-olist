/*  Orders aggregation by purchase date
    Columns:
    order_purchase_date:            Day of purchase (Format yyyy-mm-dd 00:00:00)    
    total_orders                    Total orders
    total_customers                 Total customers making orders
    total_revenue                   Total revenue - revenue considered as price of each product purchased
    average_revenue_per_order       Average revenue per order - total revenue divided by total orders
    top_1_product_category          Top 1 product category by revenue (by day)
    top_1_percent_revenue           Percent of day's revenue associated with top 1 product category
    top_2_product_category          Top 2 product category by revenue (by day)
    top_2_percent_revenue           Percent of day's revenue associated with top 2 product category
    top_3_product_category          Top 3 product category by revenue (by day)
    top_3_percent_revenue           Percent of day's revenue associated with top 3 product category

    Notes:
    (for peer reviewer)
    - I calculated separately the daily totals (orders, customers and revenue) from the daily consolidation by product
      category, and joined both totals at the final query, this way we should consider a customer that purchases
      products from different categories or from different days a single one.
    - The first part is straight forward - built a CTE with every order considering only the fields we need to extract;
    and then a CTE (daily_orders) aggregating the daily totals directly
    - The second part I split into several consecutive CTEs, in order to be clearer in each step

    (for client)
    - We considered revenue as the order item price. So total revenue is the sum of all order items prices. I noticed
    that we have the freight value available at order items dataset. Should we also consider it at revenue calculation?
    Or is there any other field to consider?
    - We pivoted the top 3 product categories and each percent by revenue (by day) to have a single row for each purchase
    date. Is that what you had in mind? Another approach we may consider is to have a row for each product category - 
    We could repeat the common values (like purchase date, total orders, total customers, revenue and average revenue)
    for each row, and have a column with rank (from 1 to 3 in this case), product category and percent from total by day.
    If we consider some day to extend it (to show more than the top 3 categories) maybe having a row for each category
    should be easier to manipulate and to analyze. Let us know if you want to test how it would look like.


    History:
    2021-05-23 crated by renzoziegler */
with 
    orders as (
        /*  Get all orders from brooklyndata.olist_orders_dataset table
            Joining with brooklyndata.olist_order_items_dataset by order_id to get each order item (each product 
            purchased within each order) and price for each item
            Joining with brooklyndata.olist_products_dataset by product_id to get product category

            Columns: 
            order_purchase_date:        Purchase date
            product_category_name       Product Category Name
            order_id                    Order id (identify uniquely each order)
            customer_id                 Customer id (identify uniquely each customer)
            order_items_price           Price for each order item (useful to calculate revenue) */
        select 
            date(orders.order_purchase_timestamp) as order_purchase_date
            , products.product_category_name as product_category_name
            , orders.order_id as order_id
            , orders.customer_id as customer_id
            , order_items.price as order_items_price
        from brooklyndata.olist_orders_dataset as orders
        inner join brooklyndata.olist_order_items_dataset as order_items on orders.order_id = order_items.order_id
        inner join brooklyndata.olist_products_dataset as products on order_items.product_id = products.product_id
    )
    , daily_orders as (
        /*  Group orders by purchase date
            Calculate total orders and total customers by counting distinct ids of each column
            Calculate total revenue by summing all order items prices

            Columns: 
            order_purchase_date:        Purchase date
            total_orders:               Count of distinct order_ids 
            total_customers:            Count of distinct customer_ids
            total_revenue:              Sum of all order items prices */
        select 
            order_purchase_date
            , count(distinct order_id) as total_orders
            , count(distinct customer_id) as total_customers
            , sum(order_items_price) as total_revenue
        from orders
        group by order_purchase_date
    )
    , daily_total_by_product_category as (
        /*  Group orders by purchase date and by product category 
            Calculate total revenue by summing all order items prices
            
            Columns: 
            order_purchase_date:            Purchase date
            product_category_name:          Product Category
            total_revenue_per_category:     Sum of all order items prices */
        select 
            order_purchase_date
            , product_category_name
            , sum(order_items_price) as total_revenue_per_category
        from orders
        group by 
            order_purchase_date
            , product_category_name
    )
    , daily_total_sorted_by_product_category as (
        /*  From total revenue for each purchase date and each product category, sort product categories by revenue 
            (by day) from highest to lowest. Also sum total revenue by day
            
            Columns: 
            order_purchase_date:            Purchase date
            product_category_name:          Product Category
            total_revenue_per_category:     Sum of all order items prices 
            product_category_sort:          Rank of product categories by revenue, partitioned by each purchase date
                                             - using row_number() function 
            total_revenue_per_day:          Sum of total revenue by day - using sum() function windowed by purchase 
                                            date */
        select 
            order_purchase_date
            , product_category_name
            , total_revenue_per_category
            , row_number() over (
                partition by order_purchase_date 
                order by total_revenue_per_category desc
              ) as product_category_sort
            , sum(total_revenue_per_category) over (
                partition by order_purchase_date
              ) as total_revenue_per_day
        from daily_total_by_product_category
    )
    , daily_top_3_product_categories as (
        /*  From previous CTE, filter top 3 product categories by day, using product_category_sort column
            Calculate percent of each category by day, dividing total revenue for each category by total revenue by day
            
            Columns: 
            order_purchase_date:            Purchase date
            product_category_name:          Product Category
            percent_revenue_per_category:   Total revenue per category divided by total_revenue_per_day
            total_revenue_per_category:     Sum of all order items prices 
            product_category_sort:          Rank of product categories by revenue */
        select
            order_purchase_date
            , product_category_name
            , total_revenue_per_category / total_revenue_per_day as percent_revenue_per_category
            , total_revenue_per_category
            , product_category_sort
        from daily_total_sorted_by_product_category
        where product_category_sort <= 3
    )
    , daily_top_3_product_categories_pivot_temp as (
        /*  From previous CTE, we got top 3 categories by revenue by purchase date
            Let's pivot it to transpose the 3 categories (one row for each category) to one single row 
            by purchase date
            We achieved it splitting each product category (first, second and third) to a different 
            column and then aggregation at the next CTE
            This is a very verbose method, and not scalable  -if we want to extend it to consider top 5
            or top 10 categories, we would have to manually create new rows for each new rank
            There are other methods we could consider, like crosstab or nest categories in an array
            But considering this example, where we defined explicitly top 3 and considering 
            a compatible method for different databases, the verbose case should suffice
            
            Columns: 
            order_purchase_date:            Purchase date
            top_1_product_category:         Column with First Product category by revenue (by day)
            top_2_product_category:         Column with Second Product category by revenue (by day) 
            top_3_product_category:         Column with Third Product category by revenue (by day) 
            top_1_percent_revenue:          Column with First Product category percent of revenue (by day)
            top_2_percent_revenue:          Column with Second Product category percent of revenue (by day) 
            top_3_percent_revenue:          Column with Third Product category percent of revenue (by day) */
        select
            order_purchase_date
            , case when product_category_sort = 1 then product_category_name else null end as top_1_product_category
            , case when product_category_sort = 2 then product_category_name else null end as top_2_product_category
            , case when product_category_sort = 3 then product_category_name else null end as top_3_product_category
            , case when product_category_sort = 1 then percent_revenue_per_category else null end as top_1_percent_revenue
            , case when product_category_sort = 2 then percent_revenue_per_category else null end as top_2_percent_revenue
            , case when product_category_sort = 3 then percent_revenue_per_category else null end as top_3_percent_revenue
        from daily_top_3_product_categories
    )
    , daily_top_3_product_categories_pivot as (
        /*  Continuing last CTE (daily_top_3_product_categories_pivot_temp) aggregating results to obtain
            a single row for each purchase date
            
            Columns: 
            order_purchase_date:            Purchase date
            top_1_product_category:         Column with First Product category by revenue (by day)
            top_2_product_category:         Column with Second Product category by revenue (by day) 
            top_3_product_category:         Column with Third Product category by revenue (by day) 
            top_1_percent_revenue:          Column with First Product category percent of revenue (by day)
            top_2_percent_revenue:          Column with Second Product category percent of revenue (by day) 
            top_3_percent_revenue:          Column with Third Product category percent of revenue (by day) */
        select
            order_purchase_date
            , max(top_1_product_category) as top_1_product_category
            , max(top_1_percent_revenue) as top_1_percent_revenue
            , max(top_2_product_category) as top_2_product_category
            , max(top_2_percent_revenue) as top_2_percent_revenue
            , max(top_3_product_category) as top_3_product_category
            , max(top_3_percent_revenue) as top_3_percent_revenue
        from daily_top_3_product_categories_pivot_temp
        group by order_purchase_date
    )
/*  Final query, joining daily_orders with daily_top_3_product_categories_pivot by purchase_date */
select 
    daily_orders.order_purchase_date
    , daily_orders.total_orders
    , daily_orders.total_customers
    , round(daily_orders.total_revenue::numeric, 2) as total_revenue
    , round((daily_orders.total_revenue / daily_orders.total_orders)::numeric, 2) as average_revenue_per_order
    , daily_top_3_product_categories_pivot.top_1_product_category
    , round(daily_top_3_product_categories_pivot.top_1_percent_revenue::numeric, 2) as top_1_percent_revenue
    , daily_top_3_product_categories_pivot.top_2_product_category
    , round(daily_top_3_product_categories_pivot.top_2_percent_revenue::numeric, 2) as top_2_percent_revenue
    , daily_top_3_product_categories_pivot.top_3_product_category
    , round(daily_top_3_product_categories_pivot.top_3_percent_revenue::numeric, 2) as top_3_percent_revenue
from daily_orders
inner join daily_top_3_product_categories_pivot on daily_orders.order_purchase_date = daily_top_3_product_categories_pivot.order_purchase_date
order by daily_orders.order_purchase_date
