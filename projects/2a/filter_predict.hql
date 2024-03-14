ADD FILE '2a.joblib';
ADD FILE 'model.py';
ADD FILE 'predict.py';


INSERT INTO TABLE hw2_pred SELECT * FROM (

    SELECT TRANSFORM * USING 'predict.py' AS (id, pred) FROM (
        SELECT (
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
            if13
        ) 
        FROM hw2_test WHERE if1 > 20 AND if1 < 40 AND NOT if1 = '' AND NOT if1 = 'NULL' AND NOT if1 = '\\N'
    )
); 