-- Fail if a currency would be excluded from the USD dashboard.
select *
from {{ ref('silver_openai_costs') }}

where currency is null
   or upper(currency) != 'USD'