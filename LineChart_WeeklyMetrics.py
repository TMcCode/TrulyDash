# Line Chart: Panelists by Number of Monthly Orders
# TO DO:
# Make Axis Percentage
# Subchart for Item Price

import psycopg2
import plotly.offline as pyo
import plotly.graph_objs as go
import numpy as np
import pandas as pd

# Create the connections
cnxn_rs = psycopg2.connect(host='sevenpdredshift.cfycmk2jdvk5.us-east-1.redshift.amazonaws.com', database='sevenpark', port='5439', user='sevenpuser', password='7Parkdata', sslmode = 'require')


query_line = """
            SELECT 'Truly' as Brand, a.orderdate-6 as orderdate, pnlPQ_DMA7, ((pnlPQ_DMA7*1.0)/(LpnlPQ_DMA7*1.0))-1 as yy_sales, pnlVol_dma7, ((pnlVOL_DMA7*1.0)/(LpnlVOL_DMA7*1.0))-1 as yy_txns,
                         (pnlitems_dma7*1.0)/(pnlVol_dma7*1.0) as AvgItems, (((pnlitems_dma7*1.0)/(pnlVol_dma7*1.0))/((lpnlitems_dma7*1.0)/(lpnlVol_dma7*1.0)))-1 as YY_AvgItems,
                         (pnlPQ_dma7*1.0)/(pnlitems_dma7*1.0) as AvgPrice, ((pnlPQ_dma7*1.0)/(pnlitems_dma7*1.0))/((lpnlPQ_dma7*1.0)/(lpnlitems_dma7*1.0))-1 as YY_AvgPrice
            FROM
                        (SELECT orderdate,
                                         sum(PQ) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as PQ_DMA7,
                                         sum(pnlPQ) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as pnlPQ_DMA7,
                                         sum(ordercount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as VOL_DMA7,
                                         sum(pnl_ordercount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as pnlVOL_DMA7,
                                         sum(item_count) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as ITEMS_DMA7,
                                         sum(pnl_itemcount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as pnlITEMS_DMA7
                        FROM
                                    (SELECT orderdate,
                                                     max(panel) as panel,
                                                     count(distinct orderid) as ordercount,
                                                     (count(distinct orderid)*1000)/(max(panel)*1.0) as pnl_ordercount,
                                                     sum(item_count) as item_count,
                                                     (sum(item_count)*1000.0)/(max(panel)*1.0) as pnl_itemcount,
                                                     sum(PQ) as PQ,
                                                     (sum(PQ)*1000.0)/(max(panel)*1.0) as pnlPQ
                                    FROM
                                            (SELECT distinct a.mailboxid, orderdate, orderid,
                                                                            max(panel) as panel,
                                                                            sum(itemquantity) as item_count,
                                                                            sum(itemprice*itemquantity) as PQ
                                            FROM khan.rawdata a
                                                     JOIN (SELECT date, max(panel) as panel FROM khan.panel_dynamic GROUP BY 1) b
                                                        ON a.orderdate = b.date
                                                     JOIN (SELECT distinct merchant FROM khan.quality_merchants) c
                                                        ON a.merchant = c.merchant
                                                     JOIN (SELECT mailboxid, SUBSTRING(shiptozip FROM 0 FOR 6) AS shiptozip FROM khan.userzips WHERE mailboxid IN (SELECT mailboxid FROM khan.userzips GROUP BY mailboxid HAVING COUNT(*) = 1)) AS userzips
                                                        ON a.mailboxid=userzips.mailboxid
                                                     JOIN (SELECT * FROM khan.cohort_list WHERE date_trunc('year',udate)::date IN(SELECT distinct date_trunc('year',orderdate) FROM khan.rawdata WHERE orderdate >= '2018-01-01') and cohort  = 'all') AS chrt
                                                        ON a.mailboxid = chrt.mailboxid AND a.orderdate = chrt.udate

                                            WHERE a.merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                                                and itemdescription ilike 'Truly%'
                                                and a.merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC')
                                                and (itemdescription ilike '%spiked%' OR itemdescription ilike '%eltz%' OR itemdescription ilike '%variet%'
                                                         OR itemdescription ilike '%Lemonade%' or itemdescription ilike '%Wild%berry%')
                                            GROUP BY 1, 2, 3)
                                    GROUP BY 1)
                        WHERE
                            orderdate <= (SELECT max(orderdate) FROM khan.rawdata)-3
                            and to_char(orderdate, 'Day') = 'Saturday') a

            JOIN

                        (SELECT orderdate,
                                         sum(PQ) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LPQ_DMA7,
                                         sum(pnlPQ) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LpnlPQ_DMA7,
                                         sum(ordercount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LVOL_DMA7,
                                         sum(pnl_ordercount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LpnlVOL_DMA7,
                                         sum(item_count) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LITEMS_DMA7,
                                         sum(pnl_itemcount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LpnlITEMS_DMA7
                        FROM
                                    (SELECT orderdate,
                                                     max(panel) as panel,
                                                     count(distinct orderid) as ordercount,
                                                     (count(distinct orderid)*1000)/(max(panel)*1.0) as pnl_ordercount,
                                                     sum(item_count) as item_count,
                                                     (sum(item_count)*1000.0)/(max(panel)*1.0) as pnl_itemcount,
                                                     sum(PQ) as PQ,
                                                     (sum(PQ)*1000.0)/(max(panel)*1.0) as pnlPQ
                                    FROM
                                            (SELECT distinct a.mailboxid, orderdate, orderid,
                                                                            max(panel) as panel,
                                                                            sum(itemquantity) as item_count,
                                                                            sum(itemprice*itemquantity) as PQ
                                            FROM khan.rawdata a
                                                     JOIN (SELECT date, max(panel) as panel FROM khan.panel_dynamic GROUP BY 1) b
                                                        ON a.orderdate = b.date
                                                     JOIN (SELECT distinct merchant FROM khan.quality_merchants) c
                                                        ON a.merchant = c.merchant
                                                     JOIN (SELECT mailboxid, SUBSTRING(shiptozip FROM 0 FOR 6) AS shiptozip FROM khan.userzips WHERE mailboxid IN (SELECT mailboxid FROM khan.userzips GROUP BY mailboxid HAVING COUNT(*) = 1)) AS userzips
                                                        ON a.mailboxid=userzips.mailboxid
                                                     JOIN (SELECT * FROM khan.cohort_list WHERE date_trunc('year',udate)::date IN(SELECT distinct date_trunc('year',orderdate) FROM khan.rawdata WHERE orderdate >= '2018-01-01') and cohort  = 'all') AS chrt
                                                        ON a.mailboxid = chrt.mailboxid AND a.orderdate = chrt.udate

                                            WHERE a.merchant IN(SELECT merchant FROM khan.sector_all WHERE sector IN('Food', 'Online Platform', 'Mass Merchant', 'Flowers/Gifts/Jewelry/Alcohol', 'Payment Processor'))
                                                and itemdescription ilike 'Truly%'
                                                and a.merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC')
                                                and (itemdescription ilike '%spiked%' OR itemdescription ilike '%eltz%' OR itemdescription ilike '%variet%'
                                                         OR itemdescription ilike '%Lemonade%' or itemdescription ilike '%Wild%berry%')
                                            GROUP BY 1, 2, 3)
                                    GROUP BY 1)
                        WHERE
                            orderdate <= (SELECT max(orderdate) FROM khan.rawdata)-3
                            and to_char(orderdate, 'Day') = 'Saturday') b
            ON a.orderdate = b.orderdate + 364
            WHERE a.orderdate >= ((CURRENT_DATE-10)-455)
            ORDER BY a.orderdate desc
"""

query_line = pd.read_sql(query_line, cnxn_rs)

trace_sales = go.Scatter(x = query_line['orderdate'], y = query_line['yy_sales'], name = 'Sales Growth',
                         line=dict(color='forestgreen', width=4))
trace_vol = go.Scatter(x = query_line['orderdate'], y = query_line['yy_txns'], name = 'Volume Growth',
                         line=dict(color='royalblue', width=4))
# trace_outside = go.Scatter(x = df_line_outside['omonth'], y = df_line_outside['active_users'])


line_growth_data = [trace_sales, trace_vol]

line_growth_layout = go.Layout(
                    yaxis= {'title': '% Growth',
                             'tickformat': ',.1%'},
                    hovermode='closest',
                    legend_orientation="h")
line_growth_fig = go.Figure(data = line_growth_data, layout = line_growth_layout)
line_growth_fig.update_layout()
#pyo.plot(line_growth_fig, filename = 'TrulyGrowth.html')


