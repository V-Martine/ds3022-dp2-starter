/* Victoria Martinez 
Part of Checkpoint C 
*/ 
with cleaned as (
    select
        try_cast(trim(order_no) as integer) as order_no,
        trim(word) as word,
        received_at
    from {{ source('raw', 'fragments') }}
),

ranked as (
    select
        order_no,
        word,
        row_number() over (
            partition by order_no
            order by received_at, word
        ) as row_num
    from cleaned
)

select order_no, word
from ranked
where row_num = 1
