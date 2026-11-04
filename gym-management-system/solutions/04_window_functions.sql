-- 04_window_functions.sql (Q36 - Q45)
-- Window Functions and Ranking

USE gym_management;

-- Q36: Rank members based on membership start date
SELECT member_id, start_date,
       RANK() OVER (ORDER BY start_date) AS member_rank
FROM tbl_member_membership;

-- Q37: Rank trainers based on number of classes taught
SELECT t.trainer_id, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, 
       COUNT(c.class_id) AS class_count,
       RANK() OVER (ORDER BY COUNT(c.class_id) DESC) AS trainer_rank
FROM tbl_trainers t
LEFT JOIN tbl_classes c ON t.trainer_id = c.trainer_id
GROUP BY t.trainer_id, trainer_name;

-- Q38: Top 3 most popular membership plans
SELECT mm.membership_id, ms.membership_type, COUNT(*) AS total_purchases
FROM tbl_member_membership mm
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
GROUP BY mm.membership_id, ms.membership_type
ORDER BY total_purchases DESC
LIMIT 3;

-- Q39: Membership revenue ranking by membership type
SELECT ms.membership_type, SUM(ms.price) AS revenue,
       RANK() OVER (ORDER BY SUM(ms.price) DESC) AS revenue_rank
FROM tbl_member_membership mm
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
GROUP BY ms.membership_type;

-- Q40: Running total of membership revenue
SELECT mm.member_membership_id,
       ms.membership_type,
       ms.price,
       SUM(ms.price) OVER (ORDER BY mm.member_membership_id) AS running_total
FROM tbl_member_membership mm
JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
LIMIT 100; -- Added limit to prevent massive output, remove if needed

-- Q41: Rank equipment based on latest maintenance date
SELECT equipment_id, equipment_name, last_maintenance_date,
       RANK() OVER (ORDER BY last_maintenance_date DESC) AS maintenance_rank
FROM tbl_equipment;

-- Q42: Most active trainer per week
SELECT t.trainer_id, CONCAT(t.first_name, ' ', t.last_name) AS trainer_name, 
       COUNT(cs.schedule_id) AS weekly_sessions,
       RANK() OVER (ORDER BY COUNT(cs.schedule_id) DESC) AS activity_rank
FROM tbl_trainers t
JOIN tbl_classes c ON t.trainer_id = c.trainer_id
JOIN tbl_class_schedule cs ON c.class_id = cs.class_id
GROUP BY t.trainer_id, trainer_name;

-- Q43: Members joined per year with ranking
SELECT YEAR(join_date) AS join_year, COUNT(*) AS members_joined,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS year_rank
FROM tbl_members
GROUP BY YEAR(join_date);

-- Q44: Top 5 members with longest memberships
SELECT mm.member_id,
       CONCAT(m.first_name, ' ', m.last_name) AS member_name,
       DATEDIFF(mm.end_date, mm.start_date) AS membership_days
FROM tbl_member_membership mm
JOIN tbl_members m ON mm.member_id = m.member_id
ORDER BY membership_days DESC
LIMIT 5;

-- Q45: Rank classes based on number of schedules
SELECT c.class_id, c.class_name, COUNT(cs.schedule_id) AS schedule_count,
       RANK() OVER (ORDER BY COUNT(cs.schedule_id) DESC) AS class_rank
FROM tbl_classes c
LEFT JOIN tbl_class_schedule cs ON c.class_id = cs.class_id
GROUP BY c.class_id, c.class_name;