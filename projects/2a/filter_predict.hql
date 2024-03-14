ADD FILE projects/2a/model.py;
ADD FILE projects/2a/predict.py;
ADD FILE 2a.joblib;

-- INSERT INTO TABLE hw2_pred SELECT TRANSFORM(*) USING './predict.py' FROM (
--         SELECT 
--             id,
--             if1,
--             if2,
--             if3,
--             if4,
--             if5,
--             if6,
--             if7,
--             if8,
--             if9,
--             if10,
--             if11,
--             if12,
--             if13
--         FROM hw2_test WHERE if1 > 20 AND if1 < 40 AND NOT if1 = '' AND NOT if1 = 'NULL' AND NOT if1 = '\\N'
--     ) t1; 

INSERT INTO TABLE hw2_pred SELECT TRANSFORM(
            id,
            if1,
            if2,
            if3,
            if4,
            if5,
            if6,
            if7,
            if8,
            if9,
            if10,
            if11,
            if12,
            if13) USING './predict.py'
        FROM hw2_test WHERE if1 > 20 AND if1 < 40 AND NOT if1 = '' AND NOT if1 = 'NULL' AND NOT if1 = '\\N'; 