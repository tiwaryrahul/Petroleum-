{{ config(materialized='table') }}

SELECT
    gl.journal_id,
    gl.posting_date,
    gl.customer_id,
    gl.product_id,
    gl.subsidiary_id,
    gl.account_number,
    COALESCE(gl.credit_amount, 0) AS revenue_amount,
    gl.quantity,
    CASE
        WHEN gl.quantity > 0 AND pm.standard_cost_per_barrel IS NOT NULL
        THEN gl.quantity * pm.standard_cost_per_barrel
        ELSE 0
    END AS standard_cost,
    COALESCE(gl.credit_amount, 0) - CASE
        WHEN gl.quantity > 0 AND pm.standard_cost_per_barrel IS NOT NULL
        THEN gl.quantity * pm.standard_cost_per_barrel
        ELSE 0
    END AS gross_margin,
    gl.created_by
FROM {{ ref('bronze_gl') }} gl
LEFT JOIN {{ ref('bronze_product_master') }} pm
    ON gl.product_id = pm.product_id
WHERE gl.customer_id IS NOT NULL
    AND COALESCE(gl.credit_amount, 0) > 0
