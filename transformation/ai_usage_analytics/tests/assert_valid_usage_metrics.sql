select *
from {{ ref('silver_openai_usage') }}

where input_tokens < 0
   or output_tokens < 0
   or model_requests < 0
   or total_tokens != input_tokens + output_tokens