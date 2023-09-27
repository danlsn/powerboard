with
    source as ( select * from {{ source('main','load_opennem__temperature')}}),

    final as (
        select distinct *
        from source
             )

select *
from final
