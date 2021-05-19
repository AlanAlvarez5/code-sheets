# MySQL Code Samples

## Case y condicionales dentro de un select

```MySQL
select (
    case 
        when not (a + b > c and b + c > a and c + a > b) then "Not A Triangle"
        when a = b and b = c then "Equilateral"
        when a = b or b = c or c = a then "Isosceles"
        else "Scalene" 
    end
) from Triangles;
```

## Concat and substring
```MySQL
select 
    concat(name, '(', substring(occupation, 1, 1), ')') 
from 
    occupations 
order by 
    name asc;

select 
    concat("There are total ", cast(count(*) as char), " ", lower(occupation), "s.") 
from 
    occupations 
group by 
    occupation 
order by 
    count(*) asc;
```