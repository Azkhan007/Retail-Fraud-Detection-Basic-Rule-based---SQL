WITH Earn_redeem
AS (
    SELECT t.[Timestamp]
        , t.memberid
        , LocationId
        , InitiatedBy
        , terminalid
        , TransactionTypeId
        , COUNT(CASE 
                WHEN TransactionTypeId = 24
                    THEN (t.ID)
                ELSE NULL
                END) AS points_earned_Trx_Count
        , COUNT(CASE 
                WHEN tm.IdTypeId = 4 AND TransactionTypeId = 24
                    THEN t.[PointsEarned]
                ELSE NULL
                END) AS P_earned_Trx_count_phone
        , COUNT(CASE 
                WHEN tm.IdTypeId = 1 AND TransactionTypeId = 24
                    THEN t.[PointsEarned]
                ELSE NULL
                END) AS P_earned_Trx_count_barcode
        , SUM(CASE 
                WHEN TransactionTypeId = 24
                    THEN (PointsEarned)
                ELSE NULL
                END) AS points_earned
        , SUM(CASE 
                WHEN tm.IdTypeId = 4 AND TransactionTypeId = 24
                    THEN t.[PointsEarned]
                ELSE NULL
                END) AS P_earned_phone
        , SUM(CASE 
                WHEN tm.IdTypeId = 1 AND TransactionTypeId = 24
                    THEN t.[PointsEarned]
                ELSE NULL
                END) AS P_earned_barcode
        , COUNT(CASE 
                WHEN TransactionTypeId = 26
                    THEN (t.ID)
                ELSE NULL
                END) AS points_redeemed_Trx_Count
        , SUM(CASE 
                WHEN TransactionTypeId = 26
                    THEN (PointsEarned)
                ELSE NULL
                END) AS points_redemeed
        , DATEADD(HOUR, DATEDIFF(HOUR, 0, LAG(t.[Timestamp]) OVER (
                    PARTITION BY t.Memberid ORDER BY t.[Timestamp]
                    )), 0) AS PreviousTimestamp
        , CASE 
            WHEN COUNT(t.[TransactionTypeId]) OVER (
                    PARTITION BY t.Memberid
                    , FLOOR(DATEDIFF(MINUTE, '19000101', t.[Timestamp]) / 60)
                    ) > 2
                AND t.TransactionTypeId = 24
                AND t.InitiatedBy <> 100
                THEN 1
            ELSE 0
            END AS Rapid_Transactions
        , COUNT(CASE 
        WHEN (DATEPART(HOUR, t.[Timestamp]) >= 23 OR DATEPART(HOUR, t.[Timestamp]) <= 3)
            AND TransactionTypeId = 24
        THEN 1
    END) OVER (PARTITION BY t.memberid ORDER BY t.[Timestamp]) AS Offhour_earned

        , COUNT(CASE 
        WHEN (DATEPART(HOUR, t.[Timestamp]) >= 23 OR DATEPART(HOUR, t.[Timestamp]) <= 3)
            AND TransactionTypeId = 26
        THEN 1
    END) AS Offhour_REDEEMED

        , COUNT(CASE 
        WHEN t.TransactionTypeId = 24
        THEN 1
    END) 
     AS m_l_c_earned
        , COUNT(CASE 
        WHEN t.TransactionTypeId = 26
        THEN 1
    END)  AS m_l_c_redeemed
    FROM Transactions t
    LEFT JOIN TransactionMemberIdType tm
        ON t.memberid = tm.memberid
            AND t.id = tm.TRANSACTIONid
    WHERE STATUS = 1
        AND TransactionTypeId IN (24, 26)
        AND CONVERT(DATE, t.[Timestamp]) > DATEADD(DAY, - 90, GETDATE())
        -- and t.memberid = 366704
    -- and t.memberid IN 6
    -- and (DATEPART(HOUR, t.[Timestamp]) >= 23 OR DATEPART(HOUR, t.[Timestamp]) <= 3)
    GROUP BY t.[Timestamp]
        , t.MemberId
        , LocationId
        , InitiatedBy
        , TerminalId
        , TransactionTypeId
        -- ORDER BY 2,1,3,4,5
    )
    -- select * from Earn_redeem
    -- WHERE MemberId = 11319
    -- order by [Timestamp] DESC
    , Expired
AS (
    SELECT [Timestamp]
        , memberid
        , LocationId
        , initiatedby
        , terminalid
        , TransactionTypeId
        , SUM(CASE 
                WHEN TransactionTypeId = 28
                    THEN (PointsEarned)
                ELSE NULL
                END) AS points_expired
        , COUNT(CASE 
                WHEN TransactionTypeId = 28
                    THEN (ID)
                ELSE NULL
                END) AS points_Expired_Trx_Count
    FROM Transactions t
    WHERE STATUS = 1
        AND TransactionTypeId = 28
        AND CONVERT(DATE, [Timestamp]) > DATEADD(DAY, - 90, GETDATE())
    GROUP BY [Timestamp]
        , MemberId
        , LocationId
        , InitiatedBy
        , TerminalId
        , TransactionTypeId
    )
    , Combined
AS (
    SELECT COALESCE(DATEADD(HOUR, DATEDIFF(HOUR, 0, er.[Timestamp]), 0), DATEADD(HOUR, DATEDIFF(HOUR, 0, e.[Timestamp]), 0)) AS Hourly_Timestamp
        , DATEADD(HOUR, + 3, COALESCE(DATEADD(HOUR, DATEDIFF(HOUR, 0, er.[Timestamp]), 0), DATEADD(HOUR, DATEDIFF(HOUR, 0, e.[Timestamp]), 0))) AS KSA_Hourly_Timestamp
        , CONVERT(DATE, DATEADD(HOUR, + 3, COALESCE(DATEADD(HOUR, DATEDIFF(HOUR, 0, er.[Timestamp]), 0), DATEADD(HOUR, DATEDIFF(HOUR, 0, e.[Timestamp]), 0)))) AS KSA_Date
        , COALESCE(er.Memberid, e.memberid) AS Memberid
        , COALESCE(er.locationid, e.locationid) AS Locationid
        , COALESCE(er.initiatedby, e.initiatedby) AS Initiatedby
        , COALESCE(er.terminalid, e.terminalid) AS Terminalid
        , SUM(er.points_earned) AS Points_Earned
        , SUM(er.points_earned_Trx_Count) AS P_earned_trx_count
        , SUM(er.P_earned_Trx_count_phone) AS P_earned_trx_count_phone
        , SUM(er.P_earned_Trx_count_barcode) AS P_earned_Trx_count_barcode
        , SUM(er.points_redemeed) AS Points_Redeemed
        , SUM(er.points_redeemed_Trx_Count) AS P_redeemed_trx_count
        , SUM(e.points_expired) AS Points_Expired
        , SUM(e.points_Expired_Trx_Count) AS P_expired_trx_count
        , SUM(er.P_earned_phone) AS P_earned_phone
        , SUM(er.P_earned_barcode) AS P_earned_barcode
        , SUM(Offhour_earned) AS ct_offhour_earned
        , SUM(Offhour_redeemed) AS ct_offhour_redeemed
        , SUM(m_l_c_earned) AS ct_m_l_c_earned
        , SUM(m_l_c_redeemed) AS ct_m_l_c_redeemed
        --, DATEDIFF(HOUR, DATEADD(HOUR, + 3, er.PreviousTimestamp), DATEADD(HOUR, + 3, COALESCE(DATEADD(HOUR, DATEDIFF(HOUR, 0, er.[Timestamp]), 0), DATEADD(HOUR, DATEDIFF(HOUR, 0, e.[Timestamp]), 0)))) AS Hours_Between_Transactions
    FROM Earn_redeem er
    FULL JOIN Expired e
        ON er.memberid = e.memberid
            AND er.[Timestamp] = e.[Timestamp]
    
    GROUP BY COALESCE(DATEADD(HOUR, DATEDIFF(HOUR, 0, er.[Timestamp]), 0), DATEADD(HOUR, DATEDIFF(HOUR, 0, e.[Timestamp]), 0))
        , DATEADD(HOUR, + 3, COALESCE(DATEADD(HOUR, DATEDIFF(HOUR, 0, er.[Timestamp]), 0), DATEADD(HOUR, DATEDIFF(HOUR, 0, e.[Timestamp]), 0)))
        , CONVERT(DATE, DATEADD(HOUR, + 3, COALESCE(DATEADD(HOUR, DATEDIFF(HOUR, 0, er.[Timestamp]), 0), DATEADD(HOUR, DATEDIFF(HOUR, 0, e.[Timestamp]), 0))))
        , COALESCE(er.Memberid, e.memberid)
        , COALESCE(er.locationid, e.locationid)
        , COALESCE(er.initiatedby, e.initiatedby)
        , COALESCE(er.terminalid, e.terminalid)
        --, PreviousTimestamp
    )
    -- select * from Combined
    -- WHERE MemberId = 11319
    -- order by 2 DESC
    , Normalized
AS (
    SELECT KSA_Hourly_Timestamp
        , KSA_Date
        , memberid
        , locationid
        , Initiatedby
        , Terminalid
        , (
            points_earned - AVG(Points_Earned) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(Points_Earned) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_pts_Earned
        , (
            ABS(Points_Redeemed) - AVG(ABS(Points_Redeemed)) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(ABS(Points_Redeemed)) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_pts_Redeemed
        , (
            P_earned_trx_count - AVG(P_earned_trx_count) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(P_earned_trx_count) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_pts_earned_trx_c
        , (
            P_redeemed_trx_count - AVG(P_redeemed_trx_count) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(P_redeemed_trx_count) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_pts_redeemed_trx_c
        , (
            P_earned_Trx_count_phone - AVG(P_earned_Trx_count_phone) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(P_earned_Trx_count_phone) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_Pts_earned_Trx_c_phone
        , (
            P_earned_Trx_count_barcode - AVG(P_earned_Trx_count_barcode) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(P_earned_Trx_count_barcode) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_Pts_earned_Trx_c_barcode
        , (
            P_earned_phone - AVG(P_earned_phone) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(P_earned_phone) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_Pts_earned_Phone
        , (
            P_earned_barcode - AVG(P_earned_barcode) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(P_earned_barcode) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_Pts_earned_barcode
        , (
            ct_offhour_earned - AVG(ct_offhour_earned) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(ct_offhour_earned) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_ct_offhour_earned
        , (
            ct_offhour_redeemed - AVG(ct_offhour_redeemed) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(ct_offhour_redeemed) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_ct_offhour_redeemed
        , (
            ct_m_l_c_earned - AVG(ct_m_l_c_earned) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(ct_m_l_c_earned) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_ct_m_l_c_earned
        , (
            ct_m_l_c_redeemed - AVG(ct_m_l_c_redeemed) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ) / NULLIF(STDEV(ct_m_l_c_redeemed) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0) AS normalized_ct_m_l_c_redeemed
        -- , (
        --     Hours_Between_Transactions - AVG(Hours_Between_Transactions) OVER (
        --         PARTITION BY memberid ORDER BY KSA_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        --         )
        --     ) / NULLIF(STDEV(Hours_Between_Transactions) OVER (
        --         PARTITION BY memberid ORDER BY KSA_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        --         ), 0) AS normalized_Hours_Between_Transactions
    FROM Combined
    WHERE CONVERT(DATE, KSA_Hourly_Timestamp) > DATEADD(DAY, - 90, GETDATE())
    )
    -- select * from Normalized
    -- WHERE Memberid = 4667109
    , FLAGS
AS (
    SELECT KSA_Hourly_Timestamp
        , KSA_Date
        , memberid
        , locationid
        , Initiatedby
        , Terminalid
        ,
        -- ABS(normalized_pts_Earned) AS normalized_pts_Earned,
        --   ABS(AVG(normalized_pts_Earned) OVER (PARTITION BY memberid ORDER BY KSA_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS avg_normalized_pts_Earned,
        --   1.5 * STDEV(normalized_pts_Earned) OVER (PARTITION BY memberid ORDER BY KSA_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS stdev_multiplier,
        CASE 
            WHEN STDEV(normalized_pts_Earned) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                THEN 0
            WHEN ABS(normalized_pts_Earned - AVG(normalized_pts_Earned) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 2 * STDEV(normalized_pts_Earned) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS pts_earned_flag
        , CASE 
            WHEN STDEV(normalized_pts_Redeemed) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                THEN 0
            WHEN ABS(normalized_pts_Redeemed - AVG(normalized_pts_Redeemed) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 2 * STDEV(normalized_pts_Redeemed) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS pts_redeemed_flag
        , CASE 
            WHEN STDEV(normalized_pts_earned_trx_c) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                AND MAX(normalized_pts_earned_trx_c) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) = 1
                THEN 0
            WHEN ABS(normalized_pts_earned_trx_c - AVG(normalized_pts_earned_trx_c) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 3 * STDEV(normalized_pts_earned_trx_c) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS pts_earned_trx_C_flag
        , CASE 
            WHEN STDEV(normalized_pts_redeemed_trx_c) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                    AND MAX(normalized_pts_redeemed_trx_c) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                THEN 0
            WHEN ABS(normalized_pts_redeemed_trx_c - AVG(normalized_pts_redeemed_trx_c) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 3 * STDEV(normalized_pts_redeemed_trx_c) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS pts_redeemed_trx_C_flag
        , CASE 
            WHEN STDEV(normalized_Pts_earned_Trx_c_phone) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                OR MAX(normalized_Pts_earned_Trx_c_phone) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) = 1
                THEN 0
            WHEN ABS(normalized_Pts_earned_Trx_c_phone - AVG(normalized_Pts_earned_Trx_c_phone) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 3 * STDEV(normalized_Pts_earned_Trx_c_phone) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS Pts_earned_Trx_c_phone_flag
        , CASE 
            WHEN STDEV(normalized_Pts_earned_Trx_c_barcode) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                OR MAX(normalized_Pts_earned_Trx_c_barcode) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) = 1
                THEN 0
            WHEN ABS(normalized_Pts_earned_Trx_c_barcode - AVG(normalized_Pts_earned_Trx_c_barcode) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 3 * STDEV(normalized_Pts_earned_Trx_c_barcode) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS Pts_earned_Trx_c_barcode_flag
        , CASE 
            WHEN STDEV(normalized_Pts_earned_Phone) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                THEN 0
            WHEN ABS(normalized_Pts_earned_Phone - AVG(normalized_Pts_earned_Phone) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 3 * STDEV(normalized_Pts_earned_Phone) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS Pts_earned_Phone_flag
        , CASE 
            WHEN STDEV(normalized_Pts_earned_barcode) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                THEN 0
            WHEN ABS(normalized_Pts_earned_barcode - AVG(normalized_Pts_earned_barcode) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 3 * STDEV(normalized_Pts_earned_barcode) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS Pts_earned_barcode_flag
        , CASE 
            WHEN STDEV(normalized_ct_offhour_earned) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                THEN 0
            WHEN ABS(normalized_ct_offhour_earned - AVG(normalized_ct_offhour_earned) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 3 * STDEV(normalized_ct_offhour_earned) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS ct_offhour_earned_flag
        , CASE 
            WHEN STDEV(normalized_ct_offhour_redeemed) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                AND MAX(normalized_ct_offhour_redeemed) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) = 1
                THEN 0
                
            WHEN ABS(normalized_ct_offhour_redeemed - AVG(normalized_ct_offhour_redeemed) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 3 * STDEV(normalized_ct_offhour_redeemed) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS ct_offhour_redeemed_flag
        , CASE 
            WHEN STDEV(normalized_ct_m_l_c_earned) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                THEN 0
            WHEN ABS(normalized_ct_m_l_c_earned - AVG(normalized_ct_m_l_c_earned) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 1.75 * STDEV(normalized_ct_m_l_c_earned) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS ct_m_l_c_earned_flag
        , CASE 
            WHEN STDEV(normalized_ct_m_l_c_redeemed) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) = 0
                AND MAX(normalized_ct_m_l_c_redeemed) OVER (
                PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) = 1
                THEN 0
            WHEN ABS(normalized_ct_m_l_c_redeemed - AVG(normalized_ct_m_l_c_redeemed) OVER (
                        PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )) > 1.5 * STDEV(normalized_ct_m_l_c_redeemed) OVER (
                    PARTITION BY memberid ORDER BY KSA_Hourly_Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                THEN 1
            ELSE 0
            END AS ct_m_l_c_redeemed_flag
        -- , CASE 
        --     WHEN STDEV(normalized_Hours_Between_Transactions) OVER (
        --             PARTITION BY memberid ORDER BY KSA_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        --             ) = 0
        --         THEN 0
        --     WHEN ABS(normalized_Hours_Between_Transactions - AVG(normalized_Hours_Between_Transactions) OVER (
        --                 PARTITION BY memberid ORDER BY KSA_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        --                 )) > 3 * STDEV(normalized_Hours_Between_Transactions) OVER (
        --             PARTITION BY memberid ORDER BY KSA_Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        --             )
        --         THEN 1
        --     ELSE 0
        --     END AS Hours_Between_Transactions_flag
    FROM normalized
    WHERE CONVERT(DATE, KSA_Hourly_Timestamp) > DATEADD(DAY, - 90, GETDATE())

        -- WHERE memberid = 2
        -- ORDER BY 1,3,4,5,6;
    )
--  ,FINAL_FLAGS AS (   
SELECT
    c.*,
    pts_earned_flag,
    pts_redeemed_flag,
    pts_earned_trx_C_flag,
    pts_redeemed_trx_C_flag,
    Pts_earned_Trx_c_phone_flag,
    Pts_earned_Trx_c_barcode_flag,
    Pts_earned_Phone_flag,
    Pts_earned_barcode_flag,
    ct_offhour_earned_flag,
    ct_offhour_redeemed_flag,
    ct_m_l_c_earned_flag,
    ct_m_l_c_redeemed_flag
-- Hours_Between_Transactions_flag
FROM FLAGS f
RIGHT JOIN Combined c
    ON f.KSA_Hourly_Timestamp = c.KSA_Hourly_Timestamp
        AND f.Memberid = c.Memberid
        AND f.Locationid = f.Locationid
        AND f.Initiatedby = C.Initiatedby
        AND f.Terminalid = C.Terminalid
WHERE 
    CONVERT(DATE, C.KSA_Hourly_Timestamp) > DATEADD(DAY, - 90, GETDATE())
        -- AND c.Memberid = 11319
    -- AND pts_earned_trx_C_flag = 1
    -- AND pts_earned_flag = 1 
    -- AND  pts_earned_trx_C_flag = 1 
    -- and Hours_Between_Transactions_flag = 1
    -- ct_m_l_c_earned_flag = 1
    -- AND ct_offhour_earned_flag = 1 
    -- AND ct_offhour_redeemed_flag = 1 
    -- AND pts_redeemed_trx_C_flag = 1
    -- group by memberid
    -- ORDER BY 2,4,5,6,7;
--  )
--  Select 
--     memberid AS Memberid,
--     SUM(Points_Earned) AS Total_Points_Earned,
--     SUM(P_earned_trx_count) AS Total_P_earned_trx_count,
--     SUM(P_earned_trx_count_phone) AS Total_P_earned_trx_count_phone,
--     SUM(P_earned_Trx_count_barcode) AS Total_P_earned_Trx_count_barcode,
--     SUM(Points_Redeemed) AS Total_Points_Redeemed,
--     SUM(P_redeemed_trx_count) AS Total_P_redeemed_trx_count,
--     SUM(Points_Expired) AS Total_Points_Expired,
--     SUM(P_expired_trx_count) AS Total_P_expired_trx_count,
--     SUM(P_earned_phone) AS Total_P_earned_phone,
--     SUM(P_earned_barcode) AS Total_P_earned_barcode,
--     MAX(ct_offhour_earned) AS ct_offhour_earned,
--     MAX(ct_offhour_redeemed) AS ct_offhour_redeemed,
--     -- SUM(ct_m_l_c_earned) AS ct_m_l_c_earned,
--     -- SUM(ct_m_l_c_redeemed) AS ct_m_l_c_redeemed,
--     COUNT(CASE WHEN pts_earned_flag = 1 THEN 1 END) AS Count_pts_earned_flag,
--     COUNT(CASE WHEN pts_redeemed_flag = 1 THEN 1 END) AS Count_pts_redeemed_flag,
--     COUNT(CASE WHEN pts_earned_trx_C_flag = 1 THEN 1 END) AS Count_pts_earned_trx_C_flag,
--     COUNT(CASE WHEN pts_redeemed_trx_C_flag = 1 THEN 1 END) AS Count_pts_redeemed_trx_C_flag,
--     COUNT(CASE WHEN Pts_earned_Trx_c_phone_flag = 1 THEN 1 END) AS Count_Pts_earned_Trx_c_phone_flag,
--     COUNT(CASE WHEN Pts_earned_Trx_c_barcode_flag = 1 THEN 1 END) AS Count_Pts_earned_Trx_c_barcode_flag,
--     COUNT(CASE WHEN Pts_earned_Phone_flag = 1 THEN 1 END) AS Count_Pts_earned_Phone_flag,
--     COUNT(CASE WHEN Pts_earned_barcode_flag = 1 THEN 1 END) AS Count_Pts_earned_barcode_flag,
--     COUNT(CASE WHEN ct_offhour_earned_flag = 1 THEN 1 END) AS Count_ct_offhour_earned_flag,
--     COUNT(CASE WHEN ct_offhour_redeemed_flag = 1 THEN 1 END) AS Count_ct_offhour_redeemed_flag,
--     COUNT(CASE WHEN ct_m_l_c_earned_flag = 1 THEN 1 END) AS Count_ct_m_l_c_earned_flag,
--     COUNT(CASE WHEN ct_m_l_c_redeemed_flag = 1 THEN 1 END) AS Count_ct_m_l_c_redeemed_flag
-- FROM 
--     FINAL_FLAGS FF
-- GROUP BY Memberid 
-- ORDER BY Memberid


