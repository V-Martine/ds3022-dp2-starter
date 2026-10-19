/* Victoria Martinez 
Part of Checkpoint C 
*/ 
with cleaned as (
    select
        try_cast(trim(order_no) as integer) as order_no,
        trim(word) as word,
        received_at
    from {{ source('raw', 'fragments') }}
)

select distinct
    order_no,
    word
from cleaned
