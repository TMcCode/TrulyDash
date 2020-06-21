# Truly 12-Pack Pricing by MSA over the past 30 Days
# To Do:
# -- Labels: Add Title; Show Hover, Legend as Dollars; Change name of avg_price to Average Price
# -- Dynamic Gradient
# -- Show Y/Y Dollars as a Hover
# -- Add Slider for Time Series Change

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
cnxn_rs = psycopg2.connect(host='sevenpdredshift.cgxitduwhaqr.us-east-1.redshift.amazonaws.com', database='sevenpark', port='5439', user='sevenpuser', password='7Parkdata', sslmode = 'require')
# df_inc = pd.read_csv('/Users/tim/Dropbox/HelperQueries/IncomebyZip.csv')
# df_inc['zipcode'] = df_inc.zipcode.astype('category')

# Load ZiptoFIPS data
df_inc = pd.read_csv('/Users/tim/Dropbox/Demographics/FIPSCounty.csv')
df_inc = df_inc.dropna(subset=['fips'])
df_inc['fips'] = pd.Series(map(lambda x: '%05d' %x, df_inc['fips']))

# Get raw data by zipcode
OT_query =  """SELECT RIGHT('00000'+ISNULL(zip.fips,''),5) as fips, zip.msa as msa, avg_itemprice
FROM

(SELECT distinct fips, msa
 FROM sundance.zip_level_info2) zip

JOIN

(SELECT fips as msa, avg_itemprice
            FROM
            (SELECT agg_daily.sector_type,
                    --(date_trunc('week', cur_orderdate - interval '6d') + interval '6d')::date as oweek,
                    fips,
                          -- sum(PQ) as PQ_DMA30
                          avg(itemprice) as avg_itemprice
                          -- SUM(OT_old) as QTD_OT_Old,
                          -- SUM(PQ) as QTD_PQ,
                          -- SUM(ordercount) as QTD_OrderCount,
                          -- SUM(itemsordered) as QTD_itemsordered,
                          -- (SUM(OT)*1.0)/(SUM(ordercount)*1.0) as AOP
            FROM
                (SELECT sector_type, FIPS,
                                  max(panel) as Panel,
																	-- sum(ordertotal) as ordertotal,
                                  -- ISNULL((sum(ordertotal)::float*10000.0)/max(panel*1.0),0.0001) as OT
                                  -- (ROUND(sum(OT_old),4)*10000.0)/max(panel) as OT_Old,
                                  -- ISNULL((sum(priceqty)::float*10000.0)/max(panel*1.0),0.0001) as PQ
                                  -- (ROUND(count(distinct orderid),4)*10000.0)/max(panel) as ordercount,
                                  -- (sum(item_count)*10000.0)/max(panel) as itemsordered,
																	avg(avg_price) as itemprice
                FROM
                            (SELECT distinct 'Truly' as sector_type, a.mailboxid, orderdate, userzips.shiptozip as zip,FIPS, state_abbrev, orderid,
                                   max(panel) as panel,
                                   sum(itemquantity) as item_count, avg(itemprice) as avg_price
                            FROM (SELECT *,
																				CASE WHEN itemdescription ilike '%12 PACK%' THEN '12 Pack'
																				WHEN itemdescription ilike '%12 pack%' THEN '12 Pack'
																				WHEN itemdescription ilike '%12pk%' THEN '12 Pack'
																				WHEN itemdescription ilike '%12x%' THEN '12 Pack'
																				WHEN itemdescription ilike '%12 pk%' THEN '12 Pack'
																				WHEN itemdescription ilike '%12 cans%' THEN '12 Pack'
																				WHEN itemdescription ilike '%12 ct%' THEN '12 Pack'
																				WHEN itemdescription ilike '%12-Pack%' THEN '12 Pack'
																				WHEN itemdescription ilike '%12-12%' THEN '12 Pack'
																				ELSE 'Other' END AS CanCount
																	FROM khan.rawdata
																	WHERE CanCount = '12 Pack') a
                            JOIN
                                (SELECT date, max(panel) as panel FROM khan.panel_dynamic GROUP BY 1) b
                            ON a.orderdate = b.date
                            JOIN
                                (SELECT * FROM khan.cohort_list
                                 WHERE date_trunc('year',udate) = date_trunc('year',CURRENT_DATE-14)
                                 ) AS cohort_list
                            ON a.mailboxid = cohort_list.mailboxid and a.orderdate = cohort_list.udate
                            INNER JOIN
                                (SELECT mailboxid, SUBSTRING(shiptozip FROM 0 FOR 6) AS shiptozip
                                 FROM khan.userzips WHERE mailboxid IN
                                  (SELECT mailboxid FROM khan.userzips GROUP BY mailboxid HAVING COUNT(*) = 1)
                                 ) AS userzips
                            ON a.mailboxid=userzips.mailboxid
--                             JOIN (SELECT merchant, distribtype as sector_type
--                                   FROM khan.sector_all
--                                   WHERE distribtype = 'MLM'
--                                   ) sctr
--                             ON a.merchant = sctr.merchant
                            LEFT JOIN
                                (SELECT RIGHT('00000'+ISNULL(zipcode,''),5) as zipcode, msa as FIPS, state_abbrev FROM sundance.zip_level_info2) fips
                            ON userzips.shiptozip = fips.zipcode
                            --WHERE orderdate <= DATE(DATE_TRUNC('week', (CURRENT_DATE)) - 3)-6
                            -- *** ADJUST BELOW TO CHANGE VARIABLES ***
                                  --WHERE (merchant = 'Arbonne')
                            -- *** ADJUST ABOVE TO CHANGE VARIABLES ***
                            --WHERE (date_trunc('week', orderdate - interval '6d') + interval '6d')::date BETWEEN (CURRENT_DATE)-30 AND (CURRENT_DATE)-24
                            WHERE a.merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
																		and itemdescription ilike 'Truly%'
																		and a.merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
																												'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC')
																		and (itemdescription ilike '%spiked%' OR itemdescription ilike '%eltz%' OR itemdescription ilike '%variet%'
																				 OR itemdescription ilike '%Lemonade%' or itemdescription ilike '%Wild%berry%')
																	    and itemprice <> 0
														GROUP BY 1, 2, 3, 4,5, 6, 7)
									-- 30 Day Total
								WHERE orderdate <= (SELECT max(orderdate) FROM khan.rawdata)-3 and orderdate >= (SELECT max(orderdate) FROM khan.rawdata)-93
                GROUP BY 1, 2) agg_daily
            -- JOIN
            --     (SELECT merchant, ticker, fe_fp_start as quarterstart, fe_fp_end as quarterend FROM sundance.factset_linking lnk
            --       JOIN factset.factset_fiscal_calendar fct
            --       ON lnk.fsym_id = fct.fsym_id
            --       WHERE lnk.merchant = 'GrubHub'
            --       and metric_periodicity = 'Quarterly') dt
            -- ON  agg_daily.cur_orderdate BETWEEN dt.quarterstart and dt.quarterend
GROUP BY 1, 2
)
) rw
ON rw.msa = zip.msa
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
chloro_growth_fig = px.choropleth(df_raw, geojson=counties, locations='fips', color='avg_itemprice',
                           color_continuous_scale=["red","white","green"],
                           range_color=(10, 20),
                           scope="usa",
                           hover_name='msa',
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


chloro_growth_fig.show()

