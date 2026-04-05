SELECT
  nome,
  SUM(valor) AS total_final
FROM {{ ref('silver_vendas') }}
GROUP BY nome