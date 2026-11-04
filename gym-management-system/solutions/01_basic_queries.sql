-- 01_basic_queries.sql (Q1 - Q10)
-- Basic JOINS and Filtering

USE gym_management;

-- Q1: List all members with their current membership type
SELECT m.member_id, m.first_name, m.last_name, ms.membership_type
FROM tbl_members m
JOIN tbl_member_membership mm ON m.member_id = mm.member_id
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
WHERE CURDATE() BETWEEN mm.start_date AND mm.end_date;

-- Q2: Show member name, membership type, start_date, end_date
SELECT CONCAT(m.first_name, ' ', m.last_name) AS member_name,
       ms.membership_type, mm.start_date, mm.end_date
FROM tbl_members m
JOIN tbl_member_membership mm ON m.member_id = mm.member_id
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id;

-- Q3: List members whose membership expired
SELECT DISTINCT m.member_id, m.first_name, m.last_name, mm.end_date
FROM tbl_members m
JOIN tbl_member_membership mm ON m.member_id = mm.member_id
WHERE mm.end_date < CURDATE();

-- Q4: Show trainers and the classes they conduct
SELECT CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, c.class_name
FROM tbl_trainers t
JOIN tbl_classes c ON t.trainer_id = c.trainer_id;

-- Q5: List class schedules with trainer names
SELECT c.class_name, 
       CONCAT(t.first_name, ' ', t.last_name) AS trainer_name,
       cs.day_of_week, cs.start_time, cs.end_time
FROM tbl_class_schedule cs
JOIN tbl_classes c ON cs.class_id = c.class_id
JOIN tbl_trainers t ON c.trainer_id = t.trainer_id
ORDER BY cs.day_of_week, cs.start_time;

-- Q6: Display memberships purchased with membership price
SELECT mm.member_id, ms.membership_type, ms.price
FROM tbl_member_membership mm
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id;

-- Q7: Show classes that currently have no trainer assigned
SELECT class_id, class_name, description
FROM tbl_classes
WHERE trainer_id IS NULL;

-- Q8: Display all equipment and their last maintenance date
SELECT equipment_name, last_maintenance_date
FROM tbl_equipment;

-- Q9: Show members with multiple membership renewals
SELECT member_id, COUNT(*) AS total_memberships
FROM tbl_member_membership
GROUP BY member_id
HAVING COUNT(*) > 1;

-- Q10: List all trainers and total classes assigned
SELECT t.trainer_id, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, 
       COUNT(c.class_id) AS total_classes
FROM tbl_trainers t
LEFT JOIN tbl_classes c ON t.trainer_id = c.trainer_id
GROUP BY t.trainer_id, trainer_name;