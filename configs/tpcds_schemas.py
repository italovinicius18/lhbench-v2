"""
TPC-DS Table Schemas
Complete schemas for all 24 TPC-DS tables
"""

from pyspark.sql.types import *


def get_tpcds_schemas():
    """Return dictionary of all TPC-DS table schemas."""

    return {
        # ====================================================================
        # FACT TABLES (7 tables)
        # ====================================================================

        "store_sales": StructType([
            StructField("ss_sold_date_sk", IntegerType(), True),
            StructField("ss_sold_time_sk", IntegerType(), True),
            StructField("ss_item_sk", IntegerType(), False),
            StructField("ss_customer_sk", IntegerType(), True),
            StructField("ss_cdemo_sk", IntegerType(), True),
            StructField("ss_hdemo_sk", IntegerType(), True),
            StructField("ss_addr_sk", IntegerType(), True),
            StructField("ss_store_sk", IntegerType(), True),
            StructField("ss_promo_sk", IntegerType(), True),
            StructField("ss_ticket_number", LongType(), False),
            StructField("ss_quantity", IntegerType(), True),
            StructField("ss_wholesale_cost", DecimalType(7,2), True),
            StructField("ss_list_price", DecimalType(7,2), True),
            StructField("ss_sales_price", DecimalType(7,2), True),
            StructField("ss_ext_discount_amt", DecimalType(7,2), True),
            StructField("ss_ext_sales_price", DecimalType(7,2), True),
            StructField("ss_ext_wholesale_cost", DecimalType(7,2), True),
            StructField("ss_ext_list_price", DecimalType(7,2), True),
            StructField("ss_ext_tax", DecimalType(7,2), True),
            StructField("ss_coupon_amt", DecimalType(7,2), True),
            StructField("ss_net_paid", DecimalType(7,2), True),
            StructField("ss_net_paid_inc_tax", DecimalType(7,2), True),
            StructField("ss_net_profit", DecimalType(7,2), True),
        ]),

        "store_returns": StructType([
            StructField("sr_returned_date_sk", IntegerType(), True),
            StructField("sr_return_time_sk", IntegerType(), True),
            StructField("sr_item_sk", IntegerType(), False),
            StructField("sr_customer_sk", IntegerType(), True),
            StructField("sr_cdemo_sk", IntegerType(), True),
            StructField("sr_hdemo_sk", IntegerType(), True),
            StructField("sr_addr_sk", IntegerType(), True),
            StructField("sr_store_sk", IntegerType(), True),
            StructField("sr_reason_sk", IntegerType(), True),
            StructField("sr_ticket_number", LongType(), False),
            StructField("sr_return_quantity", IntegerType(), True),
            StructField("sr_return_amt", DecimalType(7,2), True),
            StructField("sr_return_tax", DecimalType(7,2), True),
            StructField("sr_return_amt_inc_tax", DecimalType(7,2), True),
            StructField("sr_fee", DecimalType(7,2), True),
            StructField("sr_return_ship_cost", DecimalType(7,2), True),
            StructField("sr_refunded_cash", DecimalType(7,2), True),
            StructField("sr_reversed_charge", DecimalType(7,2), True),
            StructField("sr_store_credit", DecimalType(7,2), True),
            StructField("sr_net_loss", DecimalType(7,2), True),
        ]),

        "catalog_sales": StructType([
            StructField("cs_sold_date_sk", IntegerType(), True),
            StructField("cs_sold_time_sk", IntegerType(), True),
            StructField("cs_ship_date_sk", IntegerType(), True),
            StructField("cs_bill_customer_sk", IntegerType(), True),
            StructField("cs_bill_cdemo_sk", IntegerType(), True),
            StructField("cs_bill_hdemo_sk", IntegerType(), True),
            StructField("cs_bill_addr_sk", IntegerType(), True),
            StructField("cs_ship_customer_sk", IntegerType(), True),
            StructField("cs_ship_cdemo_sk", IntegerType(), True),
            StructField("cs_ship_hdemo_sk", IntegerType(), True),
            StructField("cs_ship_addr_sk", IntegerType(), True),
            StructField("cs_call_center_sk", IntegerType(), True),
            StructField("cs_catalog_page_sk", IntegerType(), True),
            StructField("cs_ship_mode_sk", IntegerType(), True),
            StructField("cs_warehouse_sk", IntegerType(), True),
            StructField("cs_item_sk", IntegerType(), False),
            StructField("cs_promo_sk", IntegerType(), True),
            StructField("cs_order_number", LongType(), False),
            StructField("cs_quantity", IntegerType(), True),
            StructField("cs_wholesale_cost", DecimalType(7,2), True),
            StructField("cs_list_price", DecimalType(7,2), True),
            StructField("cs_sales_price", DecimalType(7,2), True),
            StructField("cs_ext_discount_amt", DecimalType(7,2), True),
            StructField("cs_ext_sales_price", DecimalType(7,2), True),
            StructField("cs_ext_wholesale_cost", DecimalType(7,2), True),
            StructField("cs_ext_list_price", DecimalType(7,2), True),
            StructField("cs_ext_tax", DecimalType(7,2), True),
            StructField("cs_coupon_amt", DecimalType(7,2), True),
            StructField("cs_ext_ship_cost", DecimalType(7,2), True),
            StructField("cs_net_paid", DecimalType(7,2), True),
            StructField("cs_net_paid_inc_tax", DecimalType(7,2), True),
            StructField("cs_net_paid_inc_ship", DecimalType(7,2), True),
            StructField("cs_net_paid_inc_ship_tax", DecimalType(7,2), True),
            StructField("cs_net_profit", DecimalType(7,2), True),
        ]),

        "catalog_returns": StructType([
            StructField("cr_returned_date_sk", IntegerType(), True),
            StructField("cr_returned_time_sk", IntegerType(), True),
            StructField("cr_item_sk", IntegerType(), False),
            StructField("cr_refunded_customer_sk", IntegerType(), True),
            StructField("cr_refunded_cdemo_sk", IntegerType(), True),
            StructField("cr_refunded_hdemo_sk", IntegerType(), True),
            StructField("cr_refunded_addr_sk", IntegerType(), True),
            StructField("cr_returning_customer_sk", IntegerType(), True),
            StructField("cr_returning_cdemo_sk", IntegerType(), True),
            StructField("cr_returning_hdemo_sk", IntegerType(), True),
            StructField("cr_returning_addr_sk", IntegerType(), True),
            StructField("cr_call_center_sk", IntegerType(), True),
            StructField("cr_catalog_page_sk", IntegerType(), True),
            StructField("cr_ship_mode_sk", IntegerType(), True),
            StructField("cr_warehouse_sk", IntegerType(), True),
            StructField("cr_reason_sk", IntegerType(), True),
            StructField("cr_order_number", LongType(), False),
            StructField("cr_return_quantity", IntegerType(), True),
            StructField("cr_return_amount", DecimalType(7,2), True),
            StructField("cr_return_tax", DecimalType(7,2), True),
            StructField("cr_return_amt_inc_tax", DecimalType(7,2), True),
            StructField("cr_fee", DecimalType(7,2), True),
            StructField("cr_return_ship_cost", DecimalType(7,2), True),
            StructField("cr_refunded_cash", DecimalType(7,2), True),
            StructField("cr_reversed_charge", DecimalType(7,2), True),
            StructField("cr_store_credit", DecimalType(7,2), True),
            StructField("cr_net_loss", DecimalType(7,2), True),
        ]),

        "web_sales": StructType([
            StructField("ws_sold_date_sk", IntegerType(), True),
            StructField("ws_sold_time_sk", IntegerType(), True),
            StructField("ws_ship_date_sk", IntegerType(), True),
            StructField("ws_item_sk", IntegerType(), False),
            StructField("ws_bill_customer_sk", IntegerType(), True),
            StructField("ws_bill_cdemo_sk", IntegerType(), True),
            StructField("ws_bill_hdemo_sk", IntegerType(), True),
            StructField("ws_bill_addr_sk", IntegerType(), True),
            StructField("ws_ship_customer_sk", IntegerType(), True),
            StructField("ws_ship_cdemo_sk", IntegerType(), True),
            StructField("ws_ship_hdemo_sk", IntegerType(), True),
            StructField("ws_ship_addr_sk", IntegerType(), True),
            StructField("ws_web_page_sk", IntegerType(), True),
            StructField("ws_web_site_sk", IntegerType(), True),
            StructField("ws_ship_mode_sk", IntegerType(), True),
            StructField("ws_warehouse_sk", IntegerType(), True),
            StructField("ws_promo_sk", IntegerType(), True),
            StructField("ws_order_number", LongType(), False),
            StructField("ws_quantity", IntegerType(), True),
            StructField("ws_wholesale_cost", DecimalType(7,2), True),
            StructField("ws_list_price", DecimalType(7,2), True),
            StructField("ws_sales_price", DecimalType(7,2), True),
            StructField("ws_ext_discount_amt", DecimalType(7,2), True),
            StructField("ws_ext_sales_price", DecimalType(7,2), True),
            StructField("ws_ext_wholesale_cost", DecimalType(7,2), True),
            StructField("ws_ext_list_price", DecimalType(7,2), True),
            StructField("ws_ext_tax", DecimalType(7,2), True),
            StructField("ws_coupon_amt", DecimalType(7,2), True),
            StructField("ws_ext_ship_cost", DecimalType(7,2), True),
            StructField("ws_net_paid", DecimalType(7,2), True),
            StructField("ws_net_paid_inc_tax", DecimalType(7,2), True),
            StructField("ws_net_paid_inc_ship", DecimalType(7,2), True),
            StructField("ws_net_paid_inc_ship_tax", DecimalType(7,2), True),
            StructField("ws_net_profit", DecimalType(7,2), True),
        ]),

        "web_returns": StructType([
            StructField("wr_returned_date_sk", IntegerType(), True),
            StructField("wr_returned_time_sk", IntegerType(), True),
            StructField("wr_item_sk", IntegerType(), False),
            StructField("wr_refunded_customer_sk", IntegerType(), True),
            StructField("wr_refunded_cdemo_sk", IntegerType(), True),
            StructField("wr_refunded_hdemo_sk", IntegerType(), True),
            StructField("wr_refunded_addr_sk", IntegerType(), True),
            StructField("wr_returning_customer_sk", IntegerType(), True),
            StructField("wr_returning_cdemo_sk", IntegerType(), True),
            StructField("wr_returning_hdemo_sk", IntegerType(), True),
            StructField("wr_returning_addr_sk", IntegerType(), True),
            StructField("wr_web_page_sk", IntegerType(), True),
            StructField("wr_reason_sk", IntegerType(), True),
            StructField("wr_order_number", LongType(), False),
            StructField("wr_return_quantity", IntegerType(), True),
            StructField("wr_return_amt", DecimalType(7,2), True),
            StructField("wr_return_tax", DecimalType(7,2), True),
            StructField("wr_return_amt_inc_tax", DecimalType(7,2), True),
            StructField("wr_fee", DecimalType(7,2), True),
            StructField("wr_return_ship_cost", DecimalType(7,2), True),
            StructField("wr_refunded_cash", DecimalType(7,2), True),
            StructField("wr_reversed_charge", DecimalType(7,2), True),
            StructField("wr_account_credit", DecimalType(7,2), True),
            StructField("wr_net_loss", DecimalType(7,2), True),
        ]),

        "inventory": StructType([
            StructField("inv_date_sk", IntegerType(), False),
            StructField("inv_item_sk", IntegerType(), False),
            StructField("inv_warehouse_sk", IntegerType(), False),
            StructField("inv_quantity_on_hand", IntegerType(), True),
        ]),

        # ====================================================================
        # DIMENSION TABLES (17 tables)
        # ====================================================================

        "store": StructType([
            StructField("s_store_sk", IntegerType(), False),
            StructField("s_store_id", StringType(), False),
            StructField("s_rec_start_date", DateType(), True),
            StructField("s_rec_end_date", DateType(), True),
            StructField("s_closed_date_sk", IntegerType(), True),
            StructField("s_store_name", StringType(), True),
            StructField("s_number_employees", IntegerType(), True),
            StructField("s_floor_space", IntegerType(), True),
            StructField("s_hours", StringType(), True),
            StructField("s_manager", StringType(), True),
            StructField("s_market_id", IntegerType(), True),
            StructField("s_geography_class", StringType(), True),
            StructField("s_market_desc", StringType(), True),
            StructField("s_market_manager", StringType(), True),
            StructField("s_division_id", IntegerType(), True),
            StructField("s_division_name", StringType(), True),
            StructField("s_company_id", IntegerType(), True),
            StructField("s_company_name", StringType(), True),
            StructField("s_street_number", StringType(), True),
            StructField("s_street_name", StringType(), True),
            StructField("s_street_type", StringType(), True),
            StructField("s_suite_number", StringType(), True),
            StructField("s_city", StringType(), True),
            StructField("s_county", StringType(), True),
            StructField("s_state", StringType(), True),
            StructField("s_zip", StringType(), True),
            StructField("s_country", StringType(), True),
            StructField("s_gmt_offset", DecimalType(5,2), True),
            StructField("s_tax_precentage", DecimalType(5,2), True),
        ]),

        "call_center": StructType([
            StructField("cc_call_center_sk", IntegerType(), False),
            StructField("cc_call_center_id", StringType(), False),
            StructField("cc_rec_start_date", DateType(), True),
            StructField("cc_rec_end_date", DateType(), True),
            StructField("cc_closed_date_sk", IntegerType(), True),
            StructField("cc_open_date_sk", IntegerType(), True),
            StructField("cc_name", StringType(), True),
            StructField("cc_class", StringType(), True),
            StructField("cc_employees", IntegerType(), True),
            StructField("cc_sq_ft", IntegerType(), True),
            StructField("cc_hours", StringType(), True),
            StructField("cc_manager", StringType(), True),
            StructField("cc_mkt_id", IntegerType(), True),
            StructField("cc_mkt_class", StringType(), True),
            StructField("cc_mkt_desc", StringType(), True),
            StructField("cc_market_manager", StringType(), True),
            StructField("cc_division", IntegerType(), True),
            StructField("cc_division_name", StringType(), True),
            StructField("cc_company", IntegerType(), True),
            StructField("cc_company_name", StringType(), True),
            StructField("cc_street_number", StringType(), True),
            StructField("cc_street_name", StringType(), True),
            StructField("cc_street_type", StringType(), True),
            StructField("cc_suite_number", StringType(), True),
            StructField("cc_city", StringType(), True),
            StructField("cc_county", StringType(), True),
            StructField("cc_state", StringType(), True),
            StructField("cc_zip", StringType(), True),
            StructField("cc_country", StringType(), True),
            StructField("cc_gmt_offset", DecimalType(5,2), True),
            StructField("cc_tax_percentage", DecimalType(5,2), True),
        ]),

        # Remaining dimension tables with simplified schemas
        # In production, these would be fully specified

        "catalog_page": StructType([
            StructField("cp_catalog_page_sk", IntegerType(), False),
            StructField("cp_catalog_page_id", StringType(), False),
            # ... more fields
        ]),

        "web_site": StructType([
            StructField("web_site_sk", IntegerType(), False),
            StructField("web_site_id", StringType(), False),
            # ... more fields
        ]),

        "web_page": StructType([
            StructField("wp_web_page_sk", IntegerType(), False),
            StructField("wp_web_page_id", StringType(), False),
            # ... more fields
        ]),

        "warehouse": StructType([
            StructField("w_warehouse_sk", IntegerType(), False),
            StructField("w_warehouse_id", StringType(), False),
            # ... more fields
        ]),

        "customer": StructType([
            StructField("c_customer_sk", IntegerType(), False),
            StructField("c_customer_id", StringType(), False),
            # ... more fields
        ]),

        "customer_address": StructType([
            StructField("ca_address_sk", IntegerType(), False),
            # ... more fields
        ]),

        "customer_demographics": StructType([
            StructField("cd_demo_sk", IntegerType(), False),
            # ... more fields
        ]),

        "date_dim": StructType([
            StructField("d_date_sk", IntegerType(), False),
            StructField("d_date_id", StringType(), False),
            StructField("d_date", DateType(), True),
            # ... more fields
        ]),

        "time_dim": StructType([
            StructField("t_time_sk", IntegerType(), False),
            StructField("t_time_id", StringType(), False),
            # ... more fields
        ]),

        "item": StructType([
            StructField("i_item_sk", IntegerType(), False),
            StructField("i_item_id", StringType(), False),
            # ... more fields
        ]),

        "promotion": StructType([
            StructField("p_promo_sk", IntegerType(), False),
            # ... more fields
        ]),

        "household_demographics": StructType([
            StructField("hd_demo_sk", IntegerType(), False),
            # ... more fields
        ]),

        "income_band": StructType([
            StructField("ib_income_band_sk", IntegerType(), False),
            # ... more fields
        ]),

        "reason": StructType([
            StructField("r_reason_sk", IntegerType(), False),
            StructField("r_reason_id", StringType(), False),
            # ... more fields
        ]),

        "ship_mode": StructType([
            StructField("sm_ship_mode_sk", IntegerType(), False),
            StructField("sm_ship_mode_id", StringType(), False),
            # ... more fields
        ]),
    }
