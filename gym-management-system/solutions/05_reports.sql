-- 05_reports.sql (Q46 - Q50)
-- Reporting Dashboards

USE gym_management;

-- Q46: Monthly membership sales report
SELECT YEAR(start_date) AS sales_year,
       MONTH(start_date) AS sales_month,
       COUNT(*) AS memberships_sold,
       SUM(ms.price) AS revenue
FROM tbl_member_membership mm
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
GROUP BY YEAR(start_date), MONTH(start_date)
ORDER BY sales_year, sales_month;

-- Q47: Yearly member growth report
SELECT YEAR(join_date) AS join_year, COUNT(*) AS new_members,
       SUM(COUNT(*)) OVER (ORDER BY YEAR(join_date)) AS cumulative_members
FROM tbl_members
GROUP BY YEAR(join_date)
ORDER BY join_year;

-- Q48: Trainer workload report
SELECT t.trainer_id, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name,
       COUNT(DISTINCT c.class_id) AS distinct_classes,
       COUNT(cs.schedule_id) AS total_schedule_slots
FROM tbl_trainers t
LEFT JOIN tbl_classes c ON t.trainer_id = c.trainer_id
LEFT JOIN tbl_class_schedule cs ON c.class_id = cs.class_id
GROUP BY t.trainer_id, trainer_name
ORDER BY total_schedule_slots DESC;

-- Q49: Equipment maintenance due report (90 days rule)
SELECT equipment_id, equipment_name, last_maintenance_date,
       DATEDIFF(CURDATE(), last_maintenance_date) AS days_since_last,
       CASE
           WHEN last_maintenance_date IS NULL THEN 'Never Maintained'
           WHEN last_maintenance_date <= CURDATE() - INTERVAL 90 DAY THEN 'OVERDUE'
           ELSE 'OK'
       END AS maintenance_status
FROM tbl_equipment
ORDER BY last_maintenance_date ASC;

-- Q50: Class timetable dashboard report
SELECT cs.day_of_week, cs.start_time, cs.end_time,
       c.class_name, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name
FROM tbl_class_schedule cs
JOIN tbl_classes c ON cs.class_id = c.class_id
JOIN tbl_trainers t ON c.trainer_id = t.trainer_id
ORDER BY FIELD(cs.day_of_week, 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'), cs.start_time;