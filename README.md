# Premium---FP-vs-Markdown-Sales
True FP sales vs markdown
library(RPostgreSQL)
library(dplyr)
# Connect to db
conn <- dbConnect(PostgreSQL(), user = '', password = '' ,
                  host = "dwh-int.infra.theiconic.com.au", port = "5439", dbname = "dwh")
raw_data <- dbGetQuery(conn,"SELECT dim_date_order_key as date,
                       product.sku_config,
                       price as rrp,
                       brand,
                       buying_category,
                       buying_sub_category,
                       round((price - (paid_price_in_bob_inc_gst))/price,2) as discount_pct,
                       stock,
                       count(distinct id_sales_order_item) as items
                       FROM
                       fact_sales_order_item item
                       JOIN dim_product product on product.dim_product_key = item.dim_product_key
                       JOIN dim_sales_order_item_status status on status.dim_sales_order_item_status_key = item.dim_sales_order_item_status_key
                       JOIN (SELECT dim_date_snapshot_key as date,
                       sku_config,
                       sum(closing_online_stock_quantity) AS stock
                       FROM fact_product_snapshot_daily snap
                       JOIN dim_product product on snap.dim_product_key = product.dim_product_key
                       WHERE dim_date_snapshot_key >=  20170101
                       GROUP BY 1,2) stock on stock.sku_config = product.sku_config AND item.dim_date_order_key = stock.date
                       WHERE status.is_before_returns_flag = 'Y'
                       AND dim_date_order_key > 20170101
                       AND rrp > 300  
                       GROUP BY 1,2,3,4,5,6,7,8
                       ORDER BY 2,1")


#New variable - Discount band_data 

clean_data <- raw_data %>%
  mutate(discount_band = (case_when(discount_pct == 0 ~ 'Full Price',
                                    discount_pct > 0 & discount_pct <= 0.1 ~ '10%',
                                    discount_pct > 0.1 & discount_pct <= 0.2 ~ '20%',
                                    discount_pct > 0.2 & discount_pct <= 0.3 ~ '30%',
                                    discount_pct > 0.3 & discount_pct <= 0.4 ~ '40%',
                                    discount_pct > 0.4 & discount_pct <= 0.5 ~ '50%',
                                    discount_pct > 0.5 & discount_pct <= 0.6 ~ '60%',
                                    discount_pct > 0.6 & discount_pct <= 0.7 ~ '70%',
                                    discount_pct > 0.7 & discount_pct <= 0.8 ~ '80%',
                                    TRUE~ '+80%')),
         rrp_band = (case_when(rrp > 300 & rrp <= 350 ~ '300-350',
                               rrp > 350 & rrp <= 400 ~ '350-400',
                               rrp > 400 & rrp <= 450 ~ '401-450',
                               rrp > 450 & rrp <= 500 ~ '450-500',
                               rrp > 500 & rrp <= 550 ~ '500-550',
                               rrp > 550 & rrp <= 600 ~ '550-600',
                               rrp > 600 & rrp <= 650 ~ '600-650',
                               rrp > 650 & rrp <= 700 ~ '650-700',
                               rrp > 700 ~ '+700')))

#Roll up
price_vs_discount <- clean_data %>%
                     group_by(rrp_band, discount_band) %>%
                     summarise(items_sold = sum(items),
                               skus = length(unique(sku_config)))

write.csv(price_vs_discount, 'price_vs_discount.csv', row.names = FALSE)

write.csv(clean_data, 'clean_data.csv', row.names = FALSE)
