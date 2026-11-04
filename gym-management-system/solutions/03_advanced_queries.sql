-- 03_advanced_queries.sql (Q26 - Q35)
-- Advanced Subqueries and Comparisons

USE gym_management;

-- Q26: Members whose membership price is above average
SELECT m.member_id, m.first_name, m.last_name, ms.membership_type, ms.price
FROM tbl_members m
JOIN tbl_member_membership mm ON m.member_id = mm.member_id
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
WHERE ms.price > (SELECT AVG(price) FROM tbl_memberships);

-- Q27: Trainers who teach more classes than average trainer
SELECT t.trainer_id, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, 
       COUNT(c.class_id) AS class_count
FROM tbl_trainers t
JOIN tbl_classes c ON t.trainer_id = c.trainer_id
GROUP BY t.trainer_id, trainer_name
HAVING COUNT(c.class_id) > (
    SELECT AVG(cnt) FROM (
        SELECT COUNT(*) AS cnt FROM tbl_classes
        WHERE trainer_id IS NOT NULL
        GROUP BY trainer_id
    ) AS avg_table
);

-- Q28: Members with latest membership end_date
SELECT mm.member_id, m.first_name, m.last_name, mm.end_date
FROM tbl_member_membership mm
JOIN tbl_members m ON mm.member_id = m.member_id
WHERE mm.end_date = (SELECT MAX(end_date) FROM tbl_member_membership);

-- Q29: Classes taught by most experienced trainer
SELECT c.class_id, c.class_name, 
       CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, t.hire_date
FROM tbl_classes c
JOIN tbl_trainers t ON c.trainer_id = t.trainer_id
WHERE t.hire_date = (SELECT MIN(hire_date) FROM tbl_trainers);

-- Q30: Members whose membership expires earliest
SELECT mm.member_id, m.first_name, m.last_name, mm.end_date
FROM tbl_member_membership mm
JOIN tbl_members m ON mm.member_id = m.member_id
WHERE mm.end_date = (SELECT MIN(end_date) FROM tbl_member_membership);

-- Q31: Equipment maintained more than twice
SELECT e.equipment_id, e.equipment_name, COUNT(em.maintenance_id) AS maintenance_count
FROM tbl_equipment e
JOIN tbl_equipment_maintenance em ON e.equipment_id = em.equipment_id
GROUP BY e.equipment_id, e.equipment_name
HAVING COUNT(em.maintenance_id) > 2;

-- Q32: Trainers hired earlier than average hire date
SELECT trainer_id, first_name, last_name, hire_date
FROM tbl_trainers
WHERE hire_date < (SELECT AVG(hire_date) FROM tbl_trainers);

-- Q33: Memberships with price greater than average membership price
SELECT membership_id, membership_type, price
FROM tbl_memberships
WHERE price > (SELECT AVG(price) FROM tbl_memberships);

-- Q34: Members whose membership duration is maximum
SELECT membership_id, membership_type, duration_in_months
FROM tbl_memberships
WHERE duration_in_months = (SELECT MAX(duration_in_months) FROM tbl_memberships);

-- Q35: Classes scheduled more than once per week
SELECT class_id, COUNT(*) AS times_per_week
FROM tbl_class_schedule
GROUP BY class_id
HAVING COUNT(*) > 1;