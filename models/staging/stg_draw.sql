with students as (
    
    select 'isabelle' as student_name union all 
    select 'alexis' union all
    select 'lishan' union all 
    select 'adrian' union all 
    select 'lola' union all 
    select 'yoann' union all 
    select 'camille'
    
)

select 
    student_name
from students
order by rand()
limit 5
