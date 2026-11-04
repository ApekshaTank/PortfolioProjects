-- 02_intermediate_queries.sql (Q11 - Q25)
-- Aggregations and Subqueries

USE gym_management;

-- Q11: Display classes with schedules and trainer details
SELECT c.class_name, c.description, 
       CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, 
       t.specialty, cs.day_of_week, cs.start_time, cs.end_time
FROM tbl_classes c
LEFT JOIN tbl_class_schedule cs ON c.class_id = cs.class_id
LEFT JOIN tbl_trainers t ON c.trainer_id = t.trainer_id;

-- Q12: List members who joined after 2020 with membership type
SELECT m.member_id, m.first_name, m.last_name, m.join_date, ms.membership_type
FROM tbl_members m
JOIN tbl_member_membership mm ON m.member_id = mm.member_id
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
WHERE m.join_date > '2020-01-01';

-- Q13: Show equipment maintenance history with equipment names
SELECT e.equipment_name, em.maintenance_date, em.description
FROM tbl_equipment e
JOIN tbl_equipment_maintenance em ON e.equipment_id = em.equipment_id
ORDER BY em.maintenance_date DESC;

-- Q14: List members who have never purchased a membership
SELECT m.member_id, m.first_name, m.last_name
FROM tbl_members m
WHERE NOT EXISTS (
    SELECT 1 FROM tbl_member_membership mm WHERE mm.member_id = m.member_id
);

-- Q15: Show class schedules happening on Monday with trainer names
SELECT c.class_name, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, 
       cs.start_time, cs.end_time
FROM tbl_class_schedule cs
JOIN tbl_classes c ON cs.class_id = c.class_id
JOIN tbl_trainers t ON c.trainer_id = t.trainer_id
WHERE cs.day_of_week = 'Monday';

-- Q16: Count total members in the gym
SELECT COUNT(*) AS total_members FROM tbl_members;

-- Q17: Count total trainers hired
SELECT COUNT(*) AS total_trainers FROM tbl_trainers;

-- Q18: Find average membership duration
SELECT AVG(duration_in_months) AS avg_duration_months FROM tbl_memberships;

-- Q19: Calculate total membership revenue
SELECT SUM(ms.price) AS total_revenue
FROM tbl_member_membership mm
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id;

-- Q20: Show revenue generated per membership type
SELECT ms.membership_type, SUM(ms.price) AS revenue
FROM tbl_member_membership mm
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
GROUP BY ms.membership_type
ORDER BY revenue DESC;

-- Q21: Find trainer with maximum classes
SELECT t.trainer_id, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, 
       COUNT(c.class_id) AS class_count
FROM tbl_trainers t
JOIN tbl_classes c ON t.trainer_id = c.trainer_id
GROUP BY t.trainer_id, trainer_name
ORDER BY class_count DESC
LIMIT 1;

-- Q22: Count total equipment available
SELECT COUNT(*) AS total_equipment FROM tbl_equipment;

-- Q23: Find number of classes per trainer
SELECT t.trainer_id, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, 
       COUNT(c.class_id) AS num_classes
FROM tbl_trainers t
LEFT JOIN tbl_classes c ON t.trainer_id = c.trainer_id
GROUP BY t.trainer_id, trainer_name;

-- Q24: Count memberships expiring this month
SELECT COUNT(*) AS expiring_this_month
FROM tbl_member_membership
WHERE MONTH(end_date) = MONTH(CURDATE())
  AND YEAR(end_date) = YEAR(CURDATE());

-- Q25: Find average age of gym members
SELECT ROUND(AVG(TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE())), 2) AS avg_age
FROM tbl_members;