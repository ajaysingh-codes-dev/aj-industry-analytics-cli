SELECT name, dep_name, salary, rnk FROM
(SELECT e.name, d.dep_name, e.salary, DENSE_RANK() OVER(
    PARTITION BY d.dep_name ORDER BY e.salary DESC) AS rnk FROM
    employees e INNER JOIN departments d ON e.dep_id = d.dep_id
) t WHERE rnk = 1;

WITH dep_avg AS(
    SELECT dep_id, ROUND(AVG(salary), 2) AS avg_salary FROM employees GROUP BY dep_id
)
SELECT e.name, e.salary, avg_salary, d.dep_name FROM dep_avg dv INNER JOIN employees e ON e.dep_id = dv.dep_id
JOIN departments d ON e.dep_id = d.dep_id
WHERE e.salary > avg_salary ORDER BY avg_salary DESC;

with exp AS(
    SELECT emp_id, TIMESTAMPDIFF(YEAR, join_date, CURDATE()) AS expe FROM employees
)
SELECT e.name, p.expe ,(CASE 
    WHEN p.expe < 3 THEN 'Bronze' WHEN p.expe BETWEEN 3 AND 5 THEN 'Silver'
    ELSE "Gold"
END) AS rnk_tier FROM employees e JOIN exp p ON e.emp_id = p.emp_id;

CREATE VIEW employees_exp_rnk AS SELECT emp_id, name, (TIMESTAMPDIFF(YEAR, join_date, CURDATE())) AS experience , (CASE 
    WHEN TIMESTAMPDIFF(YEAR, join_date, CURDATE()) < 3 THEN 'Bronze'
    WHEN TIMESTAMPDIFF(YEAR, join_date, CURDATE()) BETWEEN 3 AND 5 THEN 'Silver'
    ELSE 'Gold'
END) AS rnk_tier FROM employees;
SELECT * FROM employees_exp_rnk
WHERE rnk_tier = 'Gold';


-----------------------------------------------------------------
SELECT COUNT(*) AS emp_count FROM employees;
SELECT d.dep_name, COUNT(*) AS emp_count FROM employees e INNER JOIN departments d ON
e.dep_id = d.dep_id GROUP BY e.dep_id;

SELECT MAX(salary) AS max_salary, MIN(salary) AS min_salary,
ROUND(AVG(salary), 2) AS avg_salary FROM employees;
SELECT d.dep_name, MAX(e.salary) AS max_salary, MIN(e.salary) AS min_salary,
ROUND(AVG(e.salary), 2) AS avg_salary FROM employees e INNER JOIN departments d ON
e.dep_id = d.dep_id GROUP BY d.dep_name;

SELECT name, salary, DENSE_RANK() OVER (
ORDER BY salary DESC) AS rnk FROM employees;
CREATE OR REPLACE VIEW Salary_Ranking AS SELECT e.name, d.dep_name, e.salary, DENSE_RANK() OVER (PARTITION BY
d.dep_name ORDER BY e.salary DESC) AS rnk FROM employees e INNER JOIN
departments d ON e.dep_id = d.dep_id

SELECT name, salary FROM employees
WHERE salary > (SELECT AVG(salary) AS company_avg FROM employees);
SELECT name, dep_name, salary, dep_avg FROM
(SELECT e.name, d.dep_name, e.salary, ROUND(AVG(e.salary) OVER(
    PARTITION BY d.dep_name), 2) AS dep_avg FROM
    employees e INNER JOIN departments d ON e.dep_id = d.dep_id) t WHERE
    dep_avg < t.salary;

WITH dep_avg AS(
    SELECT e.name, e.salary, d.dep_name, ROUND(AVG(e.salary) OVER(
        PARTITION BY d.dep_name
    ), 2) AS dep_a FROM employees e INNER JOIN departments d ON
    d.dep_id = e.dep_id
)
SELECT name, salary, dep_a, (dep_a - salary) AS salary_different, dep_name FROM dep_avg

WITH ranked AS(
    SELECT e.name, e.salary, d.dep_name, DENSE_RANK() OVER(
        PARTITION BY d.dep_id ORDER BY e.salary DESC
    ) AS rnk FROM employees e INNER JOIN departments d ON
    e.dep_id = d.dep_id
)
SELECT name, salary, dep_name, rnk FROM ranked WHERE rnk <= 3;

SELECT d.dep_name, SUM(e.salary) AS total_salary FROM
employees e INNER JOIN departments d ON
e.dep_id = d.dep_id GROUP BY d.dep_name ORDER BY total_salary DESC;

SELECT r.rnk_tier, ROUND(AVG(e.salary), 2) AVG_salary FROM employees e JOIN employees_exp_rnk r
ON e.emp_id = r.emp_id GROUP BY r.rnk_tier;

SELECT * FROM employees;