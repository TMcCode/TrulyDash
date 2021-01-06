#
# To Do:
# -- Add Variable for Merchant
# -- Change to Red --> Green
# -- Labels: MSA, % of Total for Merchant, Y/Y Change
# -- Add Slider for Time Series Change
# -- Panelize Regions
# -- Dynamic Gradient

import psycopg2
import plotly.offline as pyo
import plotly.graph_objs as go
import plotly.express as px
import numpy as np
import pandas as pd
import plotly.figure_factory as ff
import sqlite3
from sqlite3 import Error
from urllib.request import urlopen
import json

# Load FIPS to Lat/Long for Chloropleth Map
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

# Create the connections
cnxn_rs = psycopg2.connect(host='sevenpdredshift.cfycmk2jdvk5.us-east-1.redshift.amazonaws.com', database='sevenpark', port='5439', user='sevenpuser', password='7Parkdata', sslmode = 'require')
# df_inc = pd.read_csv('/Users/tim/Dropbox/HelperQueries/IncomebyZip.csv')
# df_inc['zipcode'] = df_inc.zipcode.astype('category')

# Load ZiptoFIPS data
df_inc = pd.read_csv('/Users/tim/Dropbox/Demographics/FIPSCounty.csv')
df_inc = df_inc.dropna(subset=['fips'])
df_inc['fips'] = pd.Series(map(lambda x: '%05d' %x, df_inc['fips']))


# Get raw data by zipcode
OT_query =  """SELECT adds.state_abbrev as state_abbrev, Adds, Cancels, ((Cancels*1.0)/(Adds))*100 as cncl_share FROM
(SELECT state_abbrev, Vol as Adds
            FROM
            (SELECT
                    state_abbrev,
                          -- SUM(OT) as CUR_OT
                          --,
                          -- SUM(OT_old) as QTD_OT_Old,
                          -- SUM(PQ) as QTD_PQ,
                          SUM(ordercount) as Vol
                          --,
                          -- SUM(itemsordered) as QTD_itemsordered,
                          -- (SUM(OT)*1.0)/(SUM(ordercount)*1.0) as AOP
            FROM
                (SELECT orderdate as cur_orderdate, FIPS, state_abbrev,
                                  max(panel) as Panel, sum(ordertotal) as ordertotal,
                                  --ISNULL((sum(ordertotal)::float*10000.0)/max(panel*1.0),0.0001) as OT
                                  -- (ROUND(sum(OT_old),4)*10000.0)/max(panel) as OT_Old,
                                  -- (ROUND(sum(priceqty),4)*10000.0)/max(panel) as PQ,
                                  (ROUND(count(distinct orderid),4)*10000.0)/max(panel) as ordercount
																	--count(distinct orderid) as ordercountraw
                                  -- (sum(item_count)*10000.0)/max(panel) as itemsordered
                FROM
                            (SELECT distinct  a.mailboxid, orderdate, userzips.shiptozip as zip,FIPS, state_abbrev, orderid,
                                   max(panel) as panel, max(ordertotal) as ordertotal,
                                   sum(itemquantity) as item_count, sum(itemprice*itemquantity) as priceqty,
                                   CASE WHEN AVG (ordertotal) >= SUM (itemprice) THEN AVG (ordertotal) ELSE SUM (itemprice) END as OT_Old
                            FROM khan.rawdata a
                            JOIN
                                (SELECT date, max(panel) as panel FROM khan.panel_dynamic GROUP BY 1) b
                            ON a.orderdate = b.date
                            JOIN
                                (SELECT * FROM khan.cohort_list
                                -- Make sure this grabs all years in the range
                                 --WHERE date_trunc('year',udate) = date_trunc('year',CURRENT_DATE-14)
                                 ) AS cohort_list
                            ON a.mailboxid = cohort_list.mailboxid and a.orderdate = cohort_list.udate
                            INNER JOIN
                                (SELECT mailboxid, LPAD(shiptozip, 5, '0') AS shiptozip
                                 FROM khan.userzips WHERE mailboxid IN
                                  (SELECT mailboxid FROM khan.userzips GROUP BY mailboxid HAVING COUNT(*) = 1)
                                 ) AS userzips
                            ON a.mailboxid=userzips.mailboxid
                            LEFT JOIN
                                (SELECT LPAD(zipcode, 5, '0') as zipcode, msa as FIPS, state_abbrev FROM sundance.zip_level_info2) fips
                            ON userzips.shiptozip = fips.zipcode
                            --WHERE orderdate <= DATE(DATE_TRUNC('week', (CURRENT_DATE)) - 3)-6
                            -- *** ADJUST BELOW TO CHANGE VARIABLES ***
                                  WHERE merchant = 'Netflix' and
-- Cancels
-- 																				(itemdescription ilike 'we''ve canceled your membership' OR
-- 																				itemdescription ilike 'we''ve cancelled your membership' OR
-- 																				itemdescription ilike 'we''ve cancelled your Netflix Account' OR
-- 																				itemdescription ilike 'we''ve cancelled your Standard plan' OR
-- 																				itemdescription ilike 'your membership has been cancelled' OR
-- 																				itemdescription ilike 'we''ve cancelled your Unlimited Streaming plan' OR
-- 																				itemdescription ilike 'We''ve cancelled your membership' OR
-- 																				itemdescription ilike 'We''re cancelling your membership' OR
-- 																				itemdescription ilike 'we''ve canceled your Netflix Account' OR
-- 																				itemdescription ilike 'We are cancelling your membership' OR
-- 																				itemdescription ilike 'we are cancelling your membership' OR
-- 																				itemdescription ilike 'we''ve cancelled your 2 screens at a time plan' OR
-- 																				itemdescription ilike 'we''ve cancelled your Basic plan' OR
-- 																				itemdescription ilike 'we''ve cancelled your Netflix account' OR
-- 																				itemdescription ilike 'we''ve cancelled your 1 screen at a time plan' OR
-- 																				itemdescription ilike 'we''ve cancelled your % screens at a time plan' OR
-- 																				itemdescription ilike 'we''ve cancelled your Netflix Account. This change will be effective%' OR
-- 																				itemdescription ilike 'Netflix cancellation' OR
-- 																				itemdescription ilike 'We have cancelled your membership' OR
-- 																				itemdescription ilike 'we''ve cancelled your Premium plan')
-- Adds
																				 (itemdescription ilike 'Thanks for joining Netflix!' OR
																				 itemdescription ilike 'Thanks for coming back to Netflix!' OR
																				 itemdescription ilike 'Thanks for joining Netflix, you plan is%screens at a time' OR
																				 itemdescription ilike 'Welcome back to Netflix, your plan is%screens at a time' OR
																				 itemdescription ilike 'Thanks for joining Netflix, you plan is Standard' OR
																				 itemdescription ilike 'Welcome back to Netflix, your plan is Standard' OR
																				 itemdescription ilike 'Welcome back to Netflix, your plan is Basic' OR
																				 itemdescription ilike 'Thanks for joining Netflix, you plan is Premium' OR
																				 itemdescription ilike 'Thanks for joining Netflix, you plan is % screens at a time' OR
																				 itemdescription ilike 'Welcome back to Netflix, your plan is Premium' OR
																				 itemdescription ilike 'Thanks for joining Netflix, you plan is Basic' OR
																				 itemdescription ilike 'Welcome back to Netflix, your plan is % screen at a time' OR
																				 itemdescription ilike 'Thanks for joining Netflix, you plan is % screen at a time' OR
																				 itemdescription ilike 'Welcome back to Netflix, your plan is % screens at a time' OR
																				 itemdescription ilike 'welcome to Netflix!' OR
																				 itemdescription ilike 'welcome back to Netflix!' OR
																				 itemdescription ilike 'Thanks for joining Netflix.' OR
																				 itemdescription ilike 'Welcome back to Netflix!' OR
																				 itemdescription ilike 'Netflix subscription' OR
																				 itemdescription ilike 'Umlimited Streaming' OR
																				 itemdescription ilike '% screens at a time' OR
																				 itemdescription ilike '% screen at a time')
													 -- *** ADJUST ABOVE TO CHANGE VARIABLES ***
                                  and orderdate BETWEEN '1-01-2016' and '09-05-2020'
                            GROUP BY 1, 2, 3, 4,5, 6)
                GROUP BY 1, 2, 3) agg_daily
            -- JOIN
            --     (SELECT merchant, ticker, fe_fp_start as quarterstart, fe_fp_end as quarterend FROM sundance.factset_linking lnk
            --       JOIN factset.factset_fiscal_calendar fct
            --       ON lnk.fsym_id = fct.fsym_id
            --       WHERE lnk.merchant = 'GrubHub'
            --       and metric_periodicity = 'Quarterly') dt
            -- ON  agg_daily.cur_orderdate BETWEEN dt.quarterstart and dt.quarterend

            GROUP BY 1)
) adds

LEFT JOIN

(SELECT state_abbrev, Vol as Cancels
            FROM
            (SELECT
                    state_abbrev,
                          -- SUM(OT) as CUR_OT
                          --,
                          -- SUM(OT_old) as QTD_OT_Old,
                          -- SUM(PQ) as QTD_PQ,
                          SUM(ordercount) as Vol
                          --,
                          -- SUM(itemsordered) as QTD_itemsordered,
                          -- (SUM(OT)*1.0)/(SUM(ordercount)*1.0) as AOP
            FROM
                (SELECT orderdate as cur_orderdate, FIPS, state_abbrev,
                                  max(panel) as Panel, sum(ordertotal) as ordertotal,
                                  --ISNULL((sum(ordertotal)::float*10000.0)/max(panel*1.0),0.0001) as OT
                                  -- (ROUND(sum(OT_old),4)*10000.0)/max(panel) as OT_Old,
                                  -- (ROUND(sum(priceqty),4)*10000.0)/max(panel) as PQ,
                                  (ROUND(count(distinct orderid),4)*10000.0)/max(panel) as ordercount
																	--count(distinct orderid) as ordercountraw
                                  -- (sum(item_count)*10000.0)/max(panel) as itemsordered
                FROM
                            (SELECT distinct  a.mailboxid, orderdate, userzips.shiptozip as zip,FIPS, state_abbrev, orderid,
                                   max(panel) as panel, max(ordertotal) as ordertotal,
                                   sum(itemquantity) as item_count, sum(itemprice*itemquantity) as priceqty,
                                   CASE WHEN AVG (ordertotal) >= SUM (itemprice) THEN AVG (ordertotal) ELSE SUM (itemprice) END as OT_Old
                            FROM khan.rawdata a
                            JOIN
                                (SELECT date, max(panel) as panel FROM khan.panel_dynamic GROUP BY 1) b
                            ON a.orderdate = b.date
                            JOIN
                                (SELECT * FROM khan.cohort_list
                                -- Make sure this grabs all years in the range
                                 --WHERE date_trunc('year',udate) = date_trunc('year',CURRENT_DATE-14)
                                 ) AS cohort_list
                            ON a.mailboxid = cohort_list.mailboxid and a.orderdate = cohort_list.udate
                            INNER JOIN
                                (SELECT mailboxid, LPAD(shiptozip, 5, '0') AS shiptozip
                                 FROM khan.userzips WHERE mailboxid IN
                                  (SELECT mailboxid FROM khan.userzips GROUP BY mailboxid HAVING COUNT(*) = 1)
                                 ) AS userzips
                            ON a.mailboxid=userzips.mailboxid
                            LEFT JOIN
                                (SELECT LPAD(zipcode, 5, '0') as zipcode, msa as FIPS, state_abbrev FROM sundance.zip_level_info2) fips
                            ON userzips.shiptozip = fips.zipcode
                            --WHERE orderdate <= DATE(DATE_TRUNC('week', (CURRENT_DATE)) - 3)-6
                            -- *** ADJUST BELOW TO CHANGE VARIABLES ***
                                  WHERE merchant = 'Netflix' and
-- Cancels
																				(itemdescription ilike 'we''ve canceled your membership' OR
																				itemdescription ilike 'we''ve cancelled your membership' OR
																				itemdescription ilike 'we''ve cancelled your Netflix Account' OR
																				itemdescription ilike 'we''ve cancelled your Standard plan' OR
																				itemdescription ilike 'your membership has been cancelled' OR
																				itemdescription ilike 'we''ve cancelled your Unlimited Streaming plan' OR
																				itemdescription ilike 'We''ve cancelled your membership' OR
																				itemdescription ilike 'We''re cancelling your membership' OR
																				itemdescription ilike 'we''ve canceled your Netflix Account' OR
																				itemdescription ilike 'We are cancelling your membership' OR
																				itemdescription ilike 'we are cancelling your membership' OR
																				itemdescription ilike 'we''ve cancelled your 2 screens at a time plan' OR
																				itemdescription ilike 'we''ve cancelled your Basic plan' OR
																				itemdescription ilike 'we''ve cancelled your Netflix account' OR
																				itemdescription ilike 'we''ve cancelled your 1 screen at a time plan' OR
																				itemdescription ilike 'we''ve cancelled your % screens at a time plan' OR
																				itemdescription ilike 'we''ve cancelled your Netflix Account. This change will be effective%' OR
																				itemdescription ilike 'Netflix cancellation' OR
																				itemdescription ilike 'We have cancelled your membership' OR
																				itemdescription ilike 'we''ve cancelled your Premium plan')
-- Adds
-- 																				 (itemdescription ilike 'Thanks for joining Netflix!' OR
-- 																				 itemdescription ilike 'Thanks for coming back to Netflix!' OR
-- 																				 itemdescription ilike 'Thanks for joining Netflix, you plan is%screens at a time' OR
-- 																				 itemdescription ilike 'Welcome back to Netflix, your plan is%screens at a time' OR
-- 																				 itemdescription ilike 'Thanks for joining Netflix, you plan is Standard' OR
-- 																				 itemdescription ilike 'Welcome back to Netflix, your plan is Standard' OR
-- 																				 itemdescription ilike 'Welcome back to Netflix, your plan is Basic' OR
-- 																				 itemdescription ilike 'Thanks for joining Netflix, you plan is Premium' OR
-- 																				 itemdescription ilike 'Thanks for joining Netflix, you plan is % screens at a time' OR
-- 																				 itemdescription ilike 'Welcome back to Netflix, your plan is Premium' OR
-- 																				 itemdescription ilike 'Thanks for joining Netflix, you plan is Basic' OR
-- 																				 itemdescription ilike 'Welcome back to Netflix, your plan is % screen at a time' OR
-- 																				 itemdescription ilike 'Thanks for joining Netflix, you plan is % screen at a time' OR
-- 																				 itemdescription ilike 'Welcome back to Netflix, your plan is % screens at a time' OR
-- 																				 itemdescription ilike 'welcome to Netflix!' OR
-- 																				 itemdescription ilike 'welcome back to Netflix!' OR
-- 																				 itemdescription ilike 'Thanks for joining Netflix.' OR
-- 																				 itemdescription ilike 'Welcome back to Netflix!' OR
-- 																				 itemdescription ilike 'Netflix subscription' OR
-- 																				 itemdescription ilike 'Umlimited Streaming' OR
-- 																				 itemdescription ilike '% screens at a time' OR
-- 																				 itemdescription ilike '% screen at a time')
													 -- *** ADJUST ABOVE TO CHANGE VARIABLES ***
                                  and orderdate BETWEEN '08-23-2020' and '08-29-2020'
                            GROUP BY 1, 2, 3, 4,5, 6)
                GROUP BY 1, 2, 3) agg_daily
            -- JOIN
            --     (SELECT merchant, ticker, fe_fp_start as quarterstart, fe_fp_end as quarterend FROM sundance.factset_linking lnk
            --       JOIN factset.factset_fiscal_calendar fct
            --       ON lnk.fsym_id = fct.fsym_id
            --       WHERE lnk.merchant = 'GrubHub'
            --       and metric_periodicity = 'Quarterly') dt
            -- ON  agg_daily.cur_orderdate BETWEEN dt.quarterstart and dt.quarterend

            GROUP BY 1)
) cancel

ON adds.state_abbrev = cancel.state_abbrev
WHERE cncl_share > 0
ORDER BY cncl_share desc
"""
df_raw = pd.read_sql(OT_query, cnxn_rs)



#Join RawData and ZiptoFIPS Tables
# df_agg = df_raw.merge(df_inc, on='fips', how='left')
# df_agg = df_agg.dropna(subset=['fips'])

# #Sum Raw Data by FIPS
# df_FIPS = df_agg.groupby(['FIPS'], as_index = False)['contrib'].sum()
# df_FIPS['FIPS'] = pd.Series(map(lambda x: '%05d' %x, df_FIPS['FIPS']))

# States = ['GA']
#
# df_state = df_raw[df_raw['state'].isin(States)]

# Create Chloropleth
fig = px.choropleth(df_raw, locations=df_raw['state_abbrev'], locationmode = 'USA-states', color='cncl_share',
                           color_continuous_scale=["green","white","red"],
                           range_color=(0, 2),
                           scope="usa",
                           labels={'state_abbrev':'cncl_share'},
                           hover_name='state_abbrev',
                           hover_data=['cncl_share'],
                           title= '9/10-9/13 Ca'
                    )
# fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

# States = ['GA']
#
# df_sample_r = df_raw[df_raw['state'].isin(States)]
#
# values = df_sample_r['contrib']
# fips = df_sample_r['fips']
#
# colorscale = ["#171c42","#223f78","#1267b2","#4590c4","#8cb5c9","#b6bed5","#dab2be",
#               "#d79d8b","#c46852","#a63329","#701b20","#3c0911"]
#
# endpts = list(np.linspace(-75, 75, len(colorscale) - 1))
#
# fig = ff.create_choropleth(
# 	  fips = fips, values = values,
# 	  show_state_data = True, scope = States,
# 	  binning_endpoints=endpts,
# 	  county_outline={'color': 'black', 'width': 1},
# 	  state_outline={'color': 'rgb(15,15,55)', 'width': 1},
# 	  legend_title='Median Rent', title='Median Rent per Square Foot by County'
# 	  )


fig.show()