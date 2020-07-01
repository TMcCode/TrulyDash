# Line Chart: Panelists by Number of Monthly Orders
# TO DO:
# Make Both Subchart Y-Axis Percentage


import psycopg2
import plotly.offline as pyo
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd

# Create the connections
cnxn_rs = psycopg2.connect(host='sevenpdredshift.cgxitduwhaqr.us-east-1.redshift.amazonaws.com', database='sevenpark', port='5439', user='sevenpuser', password='7Parkdata', sslmode = 'require')


query_truly = """
            SELECT 'Truly' as Brand, a.orderdate-6 as weekstart, pnlPQ_DMA7, ((pnlPQ_DMA7*1.0)/(LpnlPQ_DMA7*1.0))-1 as yy_sales, pnlVol_dma7, ((pnlVOL_DMA7*1.0)/(LpnlVOL_DMA7*1.0))-1 as yy_txns,
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

df_truly = pd.read_sql(query_truly, cnxn_rs)

query_other = """
        SELECT 
                   'Other Seltzers' as Brand, a.orderdate-6 as weekstart, pnlPQ_DMA7, ((pnlPQ_DMA7*1.0)/(LpnlPQ_DMA7*1.0))-1 as YY_Sales, pnlVol_dma7, ((pnlVOL_DMA7*1.0)/(LpnlVOL_DMA7*1.0))-1 as YY_Txns, 
                     (pnlitems_dma7*1.0)/(pnlVol_dma7*1.0) as AvgItems, (((pnlitems_dma7*1.0)/(pnlVol_dma7*1.0))/((lpnlitems_dma7*1.0)/(lpnlVol_dma7*1.0)))-1 as YY_AvgItems, 
                     (pnlPQ_dma7*1.0)/(pnlitems_dma7*1.0) as AvgPrice, (((pnlpq_dma7*1.0)/(pnlitems_dma7*1.0))/((lpnlitems_dma7*1.0)/(lpnlVol_dma7*1.0)))-1 as YY_AvgPrice
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
                                                and a.merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC', 'TJ Maxx')
                                                                                            and ((itemdescription ilike '%BL%eltze' or itemdescription ilike '%Bud Light%eltze%' or itemdescription ilike '%Bud%eltze%'
                                                                                                                or itemdescription ilike '%BudLight%eltze%') OR
                                                                                                    (itemdescription ilike '%Bon%&%Viv%'
                                                                                                                or itemdescription ilike '%Bon%&%V!%' or itemdescription ilike '%BON%V!V%'
                                                                                                                or itemdescription ilike 'Bon%Viv%') OR
                                                                                                    (itemdescription ilike '%Corona%eltze%' or itemdescription ilike '%Corona%spike'
                                                                                                                or itemdescription ilike '%Coron%eltze%' or itemdescription ilike '%eltze%Corona%') OR
                                                                                                    (itemdescription ilike '%Smirnoff%eltze%') OR
                                                                                                    (((itemdescription ilike 'High Noon %') and itemdescription NOT ilike 'High Noon (Ramen)%'
                                                                                                                                                                                 and itemdescription NOT ilike 'Tibetan High Noon%')
                                                                                                                or (a.merchant = 'Hannaford' and (itemdescription ilike '%High Noon%Spiked%' 
                                                                                                                                                                    OR itemdescription ilike '%High Noon%eltze%'))) OR
                                                                                                    ((itemdescription ilike '%Hard%eltze%' OR itemdescription ilike '%Spike%eltze%')
                                                                                                     and (itemdescription NOT ilike 'White Claw%' and itemdescription NOT ilike '%White Claw%'
                                                                                                                and itemdescription NOT ilike 'Truly%'
                                                                                                                )))
                                                                                                                                                                                 
                                                                                            
                                            GROUP BY 1, 2, 3)
                                    GROUP BY 1)
                        WHERE 
                            orderdate <= (SELECT max(orderdate) FROM khan.rawdata)-3 
                            
                            ) a
                
            JOIN 
                        (
                            SELECT orderdate,
                                         sum(PQ) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LPQ_DMA7,
                                         sum(pnlPQ) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LpnlPQ_DMA7,				 
                                         sum(ordercount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LVOL_DMA7,
                                         sum(pnl_ordercount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LpnlVOL_DMA7,				 
                                         sum(item_count) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LITEMS_DMA7,
                                         sum(pnl_itemcount) OVER (ORDER BY orderdate ASC ROWS 6 PRECEDING) as LpnlITEMS_DMA7			 
                        FROM
                                  (SELECT distinct orderdate from khan.rawdata WHERE orderdate >= '2018-01-01') dt
                        FULL OUTER JOIN
                                    (SELECT orderdate as raw_date,
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
                                                and a.merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC', 'TJ Maxx')
                                                                                            and ((itemdescription ilike '%BL%eltze' or itemdescription ilike '%Bud Light%eltze%' or itemdescription ilike '%Bud%eltze%'
                                                                                                                or itemdescription ilike '%BudLight%eltze%') OR
                                                                                                    (itemdescription ilike '%Bon%&%Viv%'
                                                                                                                or itemdescription ilike '%Bon%&%V!%' or itemdescription ilike '%BON%V!V%' 
                                                                                                                or itemdescription ilike 'Bon%Viv%') OR
                                                                                                    (itemdescription ilike '%Corona%eltze%' or itemdescription ilike '%Corona%spike'
                                                                                                                or itemdescription ilike '%Coron%eltze%' or itemdescription ilike '%eltze%Corona%') OR
                                                                                                    (itemdescription ilike '%Smirnoff%eltze%') OR
                                                                                                    (((itemdescription ilike 'High Noon %') and itemdescription NOT ilike 'High Noon (Ramen)%'
                                                                                                                                                                                 and itemdescription NOT ilike 'Tibetan High Noon%')
                                                                                                                or (a.merchant = 'Hannaford' and (itemdescription ilike '%High Noon%Spiked%' 
                                                                                                                                                                    OR itemdescription ilike '%High Noon%eltze%'))) OR
                                                                                                    ((itemdescription ilike '%Hard%eltze%' OR itemdescription ilike '%Spike%eltze%')
                                                                                                     and (itemdescription NOT ilike 'White Claw%' and itemdescription NOT ilike '%White Claw%'
                                                                                                                and itemdescription NOT ilike 'Truly%'
                                                                                                                )))
                                                                                                                                                                                 
                                                                                            
                                            GROUP BY 1, 2, 3)
                                    GROUP BY 1 ORDER BY 1 desc) rw
                        ON dt.orderdate = rw.raw_date
                        WHERE 
                            orderdate <= (SELECT max(orderdate) FROM khan.rawdata)-3 
                            --and to_char(orderdate, 'Day') = 'Saturday'
                            ) b
                        
            ON a.orderdate - 364 = b.orderdate
            WHERE a.orderdate >= ((CURRENT_DATE-10)-455)
                and to_char(a.orderdate, 'Day') = 'Saturday'
            ORDER BY a.orderdate desc 
"""

df_other = pd.read_sql(query_other, cnxn_rs)

query_whiteclaw = """
          SELECT 'White Claw' as Brand, a.orderdate-6 as weekstart, pnlPQ_DMA7, ((pnlPQ_DMA7*1.0)/(LpnlPQ_DMA7*1.0))-1 as yy_sales, pnlVol_dma7, ((pnlVOL_DMA7*1.0)/(LpnlVOL_DMA7*1.0))-1 as yy_txns,
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
                                                and (itemdescription ilike 'White Claw%' or itemdescription ilike '%White Claw%')
                                                and a.merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC')
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
                                                and (itemdescription ilike 'White Claw%' or itemdescription ilike '%White Claw%')
                                                and a.merchant NOT IN('Etsy', 'eBay', 'Shutterfly', 'Target', 'FTD', 'HSN','Dillard''s', 'Costco', 'JCPenney', 'HomeAdvisor','BJ''s Wholesale Club',
                                                                                        'Shari''s Berries', 'Sears', 'zulily', 'Overstock.com', 'Walmart', 'Wish', 'PropertyRoom.com', 'Fingerhut', 'Blue Nile', 'Hallmark', 'Hollar', 'QVC')
                                            GROUP BY 1, 2, 3)
                                    GROUP BY 1)
                        WHERE
                            orderdate <= (SELECT max(orderdate) FROM khan.rawdata)-3
                            and to_char(orderdate, 'Day') = 'Saturday') b
            ON a.orderdate = b.orderdate + 364
            WHERE a.orderdate >= ((CURRENT_DATE-10)-455)
            ORDER BY a.orderdate desc
"""

df_whiteclaw = pd.read_sql(query_whiteclaw, cnxn_rs)


line_growth_fig = make_subplots(rows=2, cols=1, y_title='YY % Growth',)


trace_trulytxns = go.Scatter(x = df_truly['weekstart'], y = df_truly['yy_txns'], legendgroup="group1", name = 'Truly',
                         line=dict(color='forestgreen', width=4))
trace_wctxns = go.Scatter(x = df_whiteclaw['weekstart'], y = df_whiteclaw['yy_txns'], legendgroup="group2", name = 'White Claw',
                         line=dict(color='royalblue', width=3))
trace_othertxns = go.Scatter(x = df_other['weekstart'], y = df_other['yy_txns'], legendgroup="group3", name = 'Other',
                         line=dict(color='firebrick', width=3))

trace_trulyrev = go.Scatter(x = df_truly['weekstart'], y = df_truly['yy_sales'], legendgroup = "group1", name = 'Truly',
                         line=dict(color='forestgreen', width=4), showlegend = False)
trace_wcrev = go.Scatter(x = df_whiteclaw['weekstart'], y = df_whiteclaw['yy_sales'], legendgroup = "group2", name = 'White Claw',
                         line=dict(color='royalblue', width=3), showlegend = False)
trace_otherrev = go.Scatter(x = df_other['weekstart'], y = df_other['yy_sales'], legendgroup = "group3", name = 'Other',
                         line=dict(color='firebrick', width=3), showlegend = False)

line_growth_fig.append_trace(trace_trulytxns, row =1, col =1)
line_growth_fig.append_trace(trace_wctxns, row =1, col =1)
line_growth_fig.append_trace(trace_othertxns, row =1, col =1)
line_growth_fig.append_trace(trace_trulyrev, row =2, col =1)
line_growth_fig.append_trace(trace_wcrev, row =2, col =1)
line_growth_fig.append_trace(trace_otherrev, row =2, col =1)

# trace_outside = go.Scatter(x = df_line_outside['omonth'], y = df_line_outside['active_users'])

# line_growth_data = [trace_sales, trace_vol]

# line_growth_fig = go.Figure(data = line_growth_data, layout = line_growth_layout)

line_growth_fig.update_yaxes(title_text="Txns", row=1, col=1)
line_growth_fig.update_yaxes(title_text="Rev", row=2, col=1)


line_growth_fig.update_layout(yaxis= {
                             'tickformat': '.0%'},
                             hovermode='closest',
                             legend_orientation="h")
# pyo.plot(line_growth_fig, filename = 'TrulyGrowth.html')

#
