with source_data as (
    select *
    from workspace.spark.sparkschema

),

transformed as (

    select
        trim(employeeName) as employee_name,
        employeeId as employee_id,
        case 
            when lower(gender) in ('m','male') then 'Male'
            when lower(gender) in ('f','female') then 'Female'
            else 'Unknown'
        end as gender
    from source_data

)

select * from transformed