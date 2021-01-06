# Line Chart: Weekly Market Share by Hard Seltzer Brands
# TO DO:
# Change Colors

import psycopg2
import plotly.offline as pyo
import plotly.graph_objs as go
import numpy as np
import pandas as pd

# Create the connections
cnxn_rs = psycopg2.connect(host='sevenpdredshift.cfycmk2jdvk5.us-east-1.redshift.amazonaws.com', database='sevenpark', port='5439', user='sevenpuser', password='7Parkdata', sslmode = 'require')


query_line = """
            SELECT odate,brand, (order_count*1.0)/sum(order_count) over (PARTITION BY odate) as perc
            FROM
            (SELECT odate, brand, sum(count) as order_count
            FROM
-- Truly
            (SELECT  (date_trunc('week', orderdate - interval '6d') + interval '6d') as odate, 'Truly' as brand, merchant,
                            itemdescription, count(distinct orderid) as count
            FROM khan.rawdata
            WHERE merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                and ((itemdescription ilike 'Truly%'
                and merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC')
                and orderdate >= '2016-01-01'
                and itemdescription NOT ilike 'Truly Grass Fed%'
                and itemdescription NOT ilike 'Truly Yours by Tofutti Dessert Cones%'
                and itemdescription NOT ilike 'Truly French'
                and itemdescription NOT ilike 'Truly Natural Vitamin C%'
                and itemdescription NOT ilike 'Truly Vegan%'
                and itemdescription NOT ilike 'Truly Soft%Sheet Set%'
                and itemdescription NOT ilike 'Truly Chocolate Milk%'
                and itemdescription NOT ilike 'Truly Simple Beef%')
                OR (merchant = 'Hannaford' and (itemdescription ilike '%Truly%Spiked%' OR itemdescription ilike '%Truly%Spiked%')))
            GROUP BY 1, 2, 3,4
            HAVING (count = 1 and
                                            (itemdescription ilike 'Truly%eltz%' OR itemdescription ilike 'Truly%Spiked%' OR itemdescription ilike 'Truly%Variety%')
            ) OR (count > 1)
            UNION ALL
-- White Claw
            SELECT  (date_trunc('week', orderdate - interval '6d') + interval '6d') as odate, 'White Claw' as brand, merchant,
                            itemdescription, count(distinct orderid) as count
            FROM khan.rawdata
            WHERE merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                and (itemdescription ilike 'White Claw%' or itemdescription ilike '%White Claw%')
                and merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC', 'TJ Maxx')
                and orderdate >= '2016-01-01'
            GROUP BY 1, 2, 3,4
            UNION ALL
-- Bud Light Seltzer
            SELECT  (date_trunc('week', orderdate - interval '6d') + interval '6d') as odate, 'Bud Light Seltzer' as brand, merchant,
                            itemdescription, count(distinct orderid) as count
            FROM khan.rawdata
            WHERE merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                and (itemdescription ilike '%BL%eltze' or itemdescription ilike '%Bud Light%eltze%' or itemdescription ilike '%Bud%eltze%'
                             or itemdescription ilike '%BudLight%eltze%')
                and merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC', 'TJ Maxx')
                and orderdate >= '2016-01-01'
            GROUP BY 1, 2, 3,4
            UNION ALL
-- Bon & Viv
            SELECT  (date_trunc('week', orderdate - interval '6d') + interval '6d') as odate, 'BON & VIV' as brand, merchant,
                            itemdescription, count(distinct orderid) as count
            FROM khan.rawdata
            WHERE merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                and (itemdescription ilike '%Bon%&%Viv%'
                        or itemdescription ilike '%Bon%&%V!%' or itemdescription ilike '%BON%V!V%' or itemdescription ilike '%BON%VIV%' or itemdescription ilike 'Bon%Viv%')
                and merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC', 'TJ Maxx')
                and orderdate >= '2016-01-01'
            GROUP BY 1, 2, 3,4
            UNION ALL
-- Corona Hard Seltzer
            SELECT  (date_trunc('week', orderdate - interval '6d') + interval '6d') as odate, 'Corona Hard Seltzer' as brand, merchant,
                            itemdescription, count(distinct orderid) as count
            FROM khan.rawdata
            WHERE merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                and (itemdescription ilike '%Corona%eltze%' or itemdescription ilike '%Corona%spike'
                         or itemdescription ilike '%Coron%eltze%' or itemdescription ilike '%eltze%Corona%')
                and merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC', 'TJ Maxx')
                and orderdate >= '2016-01-01'
            GROUP BY 1, 2, 3,4
            UNION ALL
-- Smirnoff Seltzer
            SELECT  (date_trunc('week', orderdate - interval '6d') + interval '6d') as odate, 'Smirnoff Spiked Seltzer' as brand, merchant,
                            itemdescription, count(distinct orderid) as count
            FROM khan.rawdata
            WHERE merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                and (itemdescription ilike '%Smirnoff%eltze%')
                and merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar')
                and orderdate >= '2016-01-01'
            GROUP BY 1, 2, 3,4
            UNION ALL
--High Noon
            SELECT  (date_trunc('week', orderdate - interval '6d') + interval '6d') as odate, 'High Noon Sun Sips' as brand, merchant,
                            itemdescription, count(distinct orderid) as count
            FROM khan.rawdata
            WHERE merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                and ((itemdescription ilike 'High Noon %')
                and merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'TJ Maxx')
                and orderdate >= '2016-01-01'
                and itemdescription NOT ilike 'High Noon (Ramen)%'
                and itemdescription NOT ilike 'Tibetan High Noon%'
                OR (merchant = 'Hannaford' and (itemdescription ilike '%High Noon%Spiked%' OR itemdescription ilike '%High Noon%eltze%')))
            GROUP BY 1, 2, 3,4
            UNION ALL
------- Other Seltzers
            SELECT  (date_trunc('week', orderdate - interval '6d') + interval '6d') as odate, 'Other Seltzers' as brand, merchant,
                            itemdescription, count(distinct orderid) as count
            FROM khan.rawdata
            WHERE merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                and (itemdescription ilike '%Hard%eltze%' OR itemdescription ilike '%Spike%eltze%')
                and merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'TJ Maxx')
                and orderdate >= '2016-01-01'
                and (itemdescription NOT ilike 'White Claw%' and itemdescription NOT ilike '%White Claw%'
                     and itemdescription NOT ilike '%Bon%&%Viv%' and itemdescription NOT ilike '%Bon%&%V!%' and itemdescription NOT ilike '%BON%V!V%'
                         and itemdescription NOT ilike '%BL%eltze' and itemdescription NOT ilike '%Bud Light%eltze%' and itemdescription NOT ilike '%Bud%eltze%'
                         and itemdescription NOT ilike 'Truly%'
                         and itemdescription NOT ilike '%Corona%eltze%'
                         and itemdescription NOT ilike 'Smirnoff%eltze%'
                         and itemdescription NOT ilike 'High Noon %'

                         )
            GROUP BY 1, 2, 3,4)
            WHERE odate <= DATE(DATE_TRUNC('day', (CURRENT_DATE)) - 3)-9
            GROUP BY 1, 2)
            WHERE odate >= '2018-07-01'
            ORDER BY odate desc
"""

df = pd.read_sql(query_line, cnxn_rs)
df_full = df[df['brand']=='Truly']

brand = ['Truly', 'White Claw', 'Bud Light Seltzer', 'BON & VIV', 'Corona Hard Seltzer', 'Smirnoff Spiked Seltzer', 'High Noon Sun Sips', 'Other Seltzers']
line_share_data = []

for b in brand:
    trace = go.Scatter(x=df_full['odate'], y=df[df['brand']==b]['perc'], mode='lines', name = b)
    line_share_data.append(trace)

# Define the layout
line_share_layout = go.Layout(yaxis= {'title': '% Share',
                             'tickformat': ',.0%'},
                              legend_orientation="h")

#Create a fig from data and layout, and plot the fig
line_share_fig = go.Figure(data = line_share_data, layout = line_share_layout)
# pyo.plot(line_share_fig)




#
# trace_truly = go.Scatter(x = df_truly['odate'], y =df[df['brand']=='Truly']['perc'], name = 'Truly',
#                          line=dict(color='firebrick', width=4))
#
# data_line = [trace_truly]
#
# layout_line = go.Layout(title = 'Truly YY Growth',
#                     xaxis= {'title': 'Week Ending'},
#                     yaxis= {'title': 'YY % Growth'},
#                     hovermode='closest')
# fig_line = go.Figure(data = data_line, layout = layout_line)
# pyo.plot(fig_line, filename = 'TrulyGrowth.html')

# layout_line = go.Layout(title = 'Truly Share',
#                     xaxis= {'title': 'Week Ending'},
#                     yaxis= {'title': '% Share'},
#                     hovermode='closest')
# fig_line = go.Figure(data = data_line, layout = layout_line)
# pyo.plot(fig_line, filename = 'TrulyShare.html')

# data = []
#
# for b in brand:
#     # What should go inside this Scatter call?
#     trace = go.Scatter(x=df['odate'], y=df[df['brand']==b]['perc'], mode='lines', name = b)
#     data.append(trace)

# Define the layout
# layout = go.Layout(title = 'Market Share')
#
# #Create a fig from data and layout, and plot the fig
# fig = go.Figure(data = data_line, layout = layout)
# pyo.plot(fig)

#
# trace_sales = go.Scatter(x = query_line['odate'], y = query_line['perc'], name = 'Sales Growth',
#                          line=dict(color='firebrick', width=4))
# trace_vol = go.Scatter(x = query_line['odate'], y = query_line['yy_txns'], name = 'Volume Growth',
#                          line=dict(color='royalblue', width=4))
# # trace_outside = go.Scatter(x = df_line_outside['omonth'], y = df_line_outside['active_users'])
#
#
# data_line = [trace_sales, trace_vol]
#
# layout_line = go.Layout(title = 'Truly YY Growth',
#                     xaxis= {'title': 'Week Ending'},
#                     yaxis= {'title': 'YY % Growth'},
#                     hovermode='closest')
# fig_line = go.Figure(data = data_line, layout = layout_line)
# pyo.plot(fig_line, filename = 'TrulyGrowth.html')


