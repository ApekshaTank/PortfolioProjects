USE gym_management;

DELIMITER $$
-- 1 to 13: MEMBER PROCEDURES

-- 1. sp_register_new_member
CREATE PROCEDURE sp_register_new_member(
    IN p_first_name VARCHAR(100), IN p_last_name VARCHAR(100), 
    IN p_email VARCHAR(255), IN p_phone VARCHAR(20), 
    IN p_address VARCHAR(255), IN p_dob DATE, 
    IN p_membership_id INT, IN p_start_date DATE
)
BEGIN
    DECLARE v_member_id INT;
    DECLARE v_months INT;
    
    INSERT INTO tbl_members (first_name, last_name, email, phone_number, address, date_of_birth, join_date)
    VALUES (p_first_name, p_last_name, p_email, p_phone, p_address, p_dob, CURDATE());
    
    SET v_member_id = LAST_INSERT_ID();
    
    SELECT duration_in_months INTO v_months FROM tbl_memberships WHERE membership_id = p_membership_id;
    
    INSERT INTO tbl_member_membership (member_id, membership_id, start_date, end_date)
    VALUES (v_member_id, p_membership_id, p_start_date, DATE_ADD(p_start_date, INTERVAL v_months MONTH));
END$$

-- 2. sp_get_active_memberships
CREATE PROCEDURE sp_get_active_memberships()
BEGIN
    SELECT m.first_name, m.last_name, ms.membership_type, mm.start_date, mm.end_date
    FROM tbl_member_membership mm
    JOIN tbl_members m ON mm.member_id = m.member_id
    JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
    WHERE YEAR(mm.start_date) = YEAR(CURDATE()) 
      AND YEAR(mm.end_date) = YEAR(CURDATE());
END$$

-- 3. sp_renew_membership
CREATE PROCEDURE sp_renew_membership(IN p_member_id INT, IN p_membership_id INT, IN p_start_date DATE)
BEGIN
    DECLARE v_months INT;
    
    SELECT duration_in_months INTO v_months FROM tbl_memberships WHERE membership_id = p_membership_id;
    
    INSERT INTO tbl_member_membership (member_id, membership_id, start_date, end_date)
    VALUES (p_member_id, p_membership_id, p_start_date, DATE_ADD(p_start_date, INTERVAL v_months MONTH));
END$$

-- 4. sp_get_membership_history
CREATE PROCEDURE sp_get_membership_history(IN p_member_id INT)
BEGIN
    SELECT ms.membership_type, ms.price, mm.start_date, mm.end_date
    FROM tbl_member_membership mm
    JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
    WHERE mm.member_id = p_member_id;
END$$

-- 5. sp_members_without_active_membership
CREATE PROCEDURE sp_members_without_active_membership()
BEGIN
    SELECT member_id, first_name, last_name, email 
    FROM tbl_members m
    WHERE NOT EXISTS (
        SELECT 1 FROM tbl_member_membership mm 
        WHERE mm.member_id = m.member_id 
          AND CURDATE() BETWEEN mm.start_date AND mm.end_date
    );
END$$

-- 6. sp_members_by_age_group
CREATE PROCEDURE sp_members_by_age_group()
BEGIN
    SELECT 
        CASE 
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN 'Under 18'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) BETWEEN 18 AND 35 THEN '18-35'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) BETWEEN 36 AND 50 THEN '36-50'
            ELSE 'Above 50'
        END AS age_group,
        COUNT(member_id) AS total_members
    FROM tbl_members
    GROUP BY age_group;
END$$

-- 7. sp_memberships_expiring_soon
CREATE PROCEDURE sp_memberships_expiring_soon()
BEGIN
    SELECT m.first_name, m.last_name, m.email, mm.end_date
    FROM tbl_member_membership mm
    JOIN tbl_members m ON mm.member_id = m.member_id
    WHERE mm.end_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 7 DAY);
END$$

-- 8. sp_members_with_current_membership
CREATE PROCEDURE sp_members_with_current_membership()
BEGIN
    SELECT m.member_id, m.first_name, m.last_name, ms.membership_type, ms.price
    FROM tbl_members m
    JOIN tbl_member_membership mm ON m.member_id = mm.member_id
    JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
    WHERE CURDATE() BETWEEN mm.start_date AND mm.end_date;
END$$

-- 9. sp_members_no_renewal_since
CREATE PROCEDURE sp_members_no_renewal_since(IN p_date DATE)
BEGIN
    SELECT m.member_id, m.first_name, m.last_name, MAX(mm.end_date) AS last_end_date
    FROM tbl_members m
    JOIN tbl_member_membership mm ON m.member_id = mm.member_id
    GROUP BY m.member_id, m.first_name, m.last_name
    HAVING MAX(mm.end_date) < p_date;
END$$

-- 10. sp_oldest_member
CREATE PROCEDURE sp_oldest_member()
BEGIN
    SELECT first_name, last_name, date_of_birth 
    FROM tbl_members 
    ORDER BY date_of_birth ASC 
    LIMIT 1;
END$$

-- 11. sp_get_member_details
CREATE PROCEDURE sp_get_member_details(IN p_member_id INT)
BEGIN
    SELECT * FROM tbl_members WHERE member_id = p_member_id;
END$$

-- 12. sp_search_member_by_name
CREATE PROCEDURE sp_search_member_by_name(IN p_keyword VARCHAR(100))
BEGIN
    SELECT * FROM tbl_members 
    WHERE first_name LIKE CONCAT('%', p_keyword, '%') 
       OR last_name LIKE CONCAT('%', p_keyword, '%');
END$$

-- 13. sp_get_member_contact_list
CREATE PROCEDURE sp_get_member_contact_list()
BEGIN
    SELECT first_name, last_name, email, phone_number FROM tbl_members;
END$$


-- ==========================================
-- 14 to 17: MEMBERSHIP PLAN PROCEDURES
-- ==========================================

-- 14. sp_get_revenue_by_membership
CREATE PROCEDURE sp_get_revenue_by_membership()
BEGIN
    SELECT ms.membership_type, SUM(ms.price) AS revenue
    FROM tbl_member_membership mm
    JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
    GROUP BY ms.membership_type;
END$$

-- 15. sp_top_memberships
CREATE PROCEDURE sp_top_memberships()
BEGIN
    SELECT ms.membership_type, COUNT(*) AS total_bought
    FROM tbl_member_membership mm
    JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id
    GROUP BY ms.membership_type
    ORDER BY total_bought DESC;
END$$

-- 16. sp_avg_membership_duration
CREATE PROCEDURE sp_avg_membership_duration()
BEGIN
    SELECT AVG(duration_in_months) AS avg_duration FROM tbl_memberships;
END$$

-- 17. sp_total_membership_revenue
CREATE PROCEDURE sp_total_membership_revenue()
BEGIN
    SELECT SUM(ms.price) AS total_revenue
    FROM tbl_member_membership mm
    JOIN tbl_memberships ms ON mm.membership_id = ms.membership_id;
END$$



-- 18 to 30: TRAINER & CLASS PROCEDURES


-- 18. sp_assign_trainer_to_class
CREATE PROCEDURE sp_assign_trainer_to_class(IN p_class_id INT, IN p_trainer_id INT)
BEGIN
    UPDATE tbl_classes SET trainer_id = p_trainer_id WHERE class_id = p_class_id;
END$$

-- 19. sp_list_trainers_with_specialty
CREATE PROCEDURE sp_list_trainers_with_specialty()
BEGIN
    SELECT * FROM tbl_trainers ORDER BY hire_date DESC;
END$$

-- 20. sp_trainer_weekly_schedule
CREATE PROCEDURE sp_trainer_weekly_schedule(IN p_trainer_id INT)
BEGIN
    SELECT cs.day_of_week, cs.start_time, cs.end_time, c.class_name
    FROM tbl_class_schedule cs
    JOIN tbl_classes c ON cs.class_id = c.class_id
    WHERE c.trainer_id = p_trainer_id;
END$$

-- 21. sp_trainer_workload
CREATE PROCEDURE sp_trainer_workload()
BEGIN
    SELECT t.first_name, t.last_name, 
           COUNT(DISTINCT c.class_id) AS classes_count, 
           COUNT(cs.schedule_id) AS schedule_count
    FROM tbl_trainers t
    LEFT JOIN tbl_classes c ON t.trainer_id = c.trainer_id
    LEFT JOIN tbl_class_schedule cs ON c.class_id = cs.class_id
    GROUP BY t.trainer_id, t.first_name, t.last_name;
END$$

-- 22. sp_insert_trainer
CREATE PROCEDURE sp_insert_trainer(
    IN p_first VARCHAR(100), IN p_last VARCHAR(100), IN p_email VARCHAR(255), 
    IN p_phone VARCHAR(20), IN p_specialty VARCHAR(100), IN p_hire_date DATE
)
BEGIN
    INSERT INTO tbl_trainers (first_name, last_name, email, phone_number, specialty, hire_date)
    VALUES (p_first, p_last, p_email, p_phone, p_specialty, p_hire_date);
END$$

-- 23. sp_insert_update_trainer
CREATE PROCEDURE sp_insert_update_trainer(
    IN p_id INT, IN p_first VARCHAR(100), IN p_last VARCHAR(100), IN p_email VARCHAR(255), 
    IN p_phone VARCHAR(20), IN p_specialty VARCHAR(100), IN p_hire_date DATE
)
BEGIN
    IF EXISTS (SELECT 1 FROM tbl_trainers WHERE trainer_id = p_id) THEN
        UPDATE tbl_trainers 
        SET first_name = p_first, last_name = p_last, email = p_email, 
            phone_number = p_phone, specialty = p_specialty, hire_date = p_hire_date
        WHERE trainer_id = p_id;
    ELSE
        INSERT INTO tbl_trainers (first_name, last_name, email, phone_number, specialty, hire_date)
        VALUES (p_first, p_last, p_email, p_phone, p_specialty, p_hire_date);
    END IF;
END$$

-- 24. sp_delete_trainer
CREATE PROCEDURE sp_delete_trainer(IN p_trainer_id INT)
BEGIN
    UPDATE tbl_classes SET trainer_id = NULL WHERE trainer_id = p_trainer_id;
    DELETE FROM tbl_trainers WHERE trainer_id = p_trainer_id;
END$$

-- 25. sp_get_trainer_by_id
CREATE PROCEDURE sp_get_trainer_by_id(IN p_trainer_id INT)
BEGIN
    SELECT * FROM tbl_trainers WHERE trainer_id = p_trainer_id;
END$$

-- 26. sp_get_class_schedule
CREATE PROCEDURE sp_get_class_schedule(IN p_class_id INT)
BEGIN
    SELECT c.class_name, t.first_name AS trainer_name, cs.day_of_week, cs.start_time, cs.end_time
    FROM tbl_class_schedule cs
    JOIN tbl_classes c ON cs.class_id = c.class_id
    LEFT JOIN tbl_trainers t ON c.trainer_id = t.trainer_id
    WHERE c.class_id = p_class_id;
END$$

-- 27. sp_list_classes_with_trainers
CREATE PROCEDURE sp_list_classes_with_trainers()
BEGIN
    SELECT c.class_name, t.first_name AS trainer_name
    FROM tbl_classes c
    LEFT JOIN tbl_trainers t ON c.trainer_id = t.trainer_id;
END$$

-- 28. sp_get_class_by_id
CREATE PROCEDURE sp_get_class_by_id(IN p_class_id INT)
BEGIN
    SELECT * FROM tbl_classes WHERE class_id = p_class_id;
END$$

-- 29. sp_insert_update_class
CREATE PROCEDURE sp_insert_update_class(
    IN p_id INT, IN p_class_name VARCHAR(100), IN p_desc VARCHAR(255), IN p_trainer_id INT
)
BEGIN
    IF p_id IS NULL OR p_id = 0 THEN
        INSERT INTO tbl_classes (class_name, description, trainer_id) 
        VALUES (p_class_name, p_desc, p_trainer_id);
    ELSE
        UPDATE tbl_classes 
        SET class_name = p_class_name, description = p_desc, trainer_id = p_trainer_id 
        WHERE class_id = p_id;
    END IF;
END$$

-- 30. sp_delete_class
CREATE PROCEDURE sp_delete_class(IN p_class_id INT)
BEGIN
    DELETE FROM tbl_class_schedule WHERE class_id = p_class_id;
    DELETE FROM tbl_classes WHERE class_id = p_class_id;
END$$



-- 31 to 36: EQUIPMENT, TIMETABLE & VALIDATION


-- 31. sp_upcoming_maintenance
CREATE PROCEDURE sp_upcoming_maintenance()
BEGIN
    SELECT * FROM tbl_equipment 
    WHERE last_maintenance_date <= CURDATE() - INTERVAL 75 DAY;
END$$

-- 32. sp_equipment_due_maintenance
CREATE PROCEDURE sp_equipment_due_maintenance()
BEGIN
    SELECT * FROM tbl_equipment 
    WHERE MONTH(last_maintenance_date) != MONTH(CURDATE()) 
       OR YEAR(last_maintenance_date) != YEAR(CURDATE());
END$$

-- 33. sp_equipment_maintenance_log
CREATE PROCEDURE sp_equipment_maintenance_log(IN p_equipment_id INT)
BEGIN
    SELECT e.equipment_name, em.maintenance_date, em.description
    FROM tbl_equipment e
    JOIN tbl_equipment_maintenance em ON e.equipment_id = em.equipment_id
    WHERE e.equipment_id = p_equipment_id;
END$$

-- 34. sp_update_equipment_description
CREATE PROCEDURE sp_update_equipment_description(IN p_id INT, IN p_desc VARCHAR(255))
BEGIN
    UPDATE tbl_equipment SET description = p_desc WHERE equipment_id = p_id;
END$$

-- 35. sp_todays_classes
CREATE PROCEDURE sp_todays_classes()
BEGIN
    SELECT c.class_name, t.first_name AS trainer_name, cs.start_time, cs.end_time
    FROM tbl_class_schedule cs
    JOIN tbl_classes c ON cs.class_id = c.class_id
    LEFT JOIN tbl_trainers t ON c.trainer_id = t.trainer_id
    WHERE cs.day_of_week = DAYNAME(CURDATE());
END$$

-- 36. sp_check_duplicate_email
CREATE PROCEDURE sp_check_duplicate_email(IN p_email VARCHAR(255))
BEGIN
    IF EXISTS (SELECT 1 FROM tbl_members WHERE email = p_email) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error: Email already exists.';
    ELSE
        SELECT 'Email is available' AS result;
    END IF;
END$$

DELIMITER ;