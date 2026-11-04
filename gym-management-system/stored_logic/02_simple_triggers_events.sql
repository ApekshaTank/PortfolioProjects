USE gym_management;
DELIMITER $$

-- TRIGGER 1: Auto-update last_maintenance_date
CREATE TRIGGER trg_update_last_maintenance
AFTER INSERT ON tbl_equipment_maintenance
FOR EACH ROW
BEGIN
    UPDATE tbl_equipment 
    SET last_maintenance_date = NEW.maintenance_date
    WHERE equipment_id = NEW.equipment_id;
END$$

-- TRIGGER 2: Prevent overlapping schedule
CREATE TRIGGER trg_prevent_overlapping_schedule
BEFORE INSERT ON tbl_class_schedule
FOR EACH ROW
BEGIN
    DECLARE v_trainer INT;
    
    SELECT trainer_id INTO v_trainer FROM tbl_classes WHERE class_id = NEW.class_id;
    
    IF EXISTS (
        SELECT 1 
        FROM tbl_class_schedule cs
        JOIN tbl_classes c ON cs.class_id = c.class_id
        WHERE c.trainer_id = v_trainer
          AND cs.day_of_week = NEW.day_of_week
          AND cs.start_time = NEW.start_time
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error: Trainer is already booked at this time!';
    END IF;
END$$

DELIMITER ;

-- EVENTS (Make sure event scheduler is ON)
SET GLOBAL event_scheduler = ON;
DELIMITER $$

-- EVENT 1
CREATE EVENT ev_equipment_maintenance_reminder
ON SCHEDULE EVERY 1 DAY
DO
BEGIN
    INSERT INTO tbl_logs (log_type, message)
    SELECT 'MAINTENANCE', CONCAT('Equipment ', equipment_name, ' needs maintenance.')
    FROM tbl_equipment
    WHERE last_maintenance_date <= CURDATE() - INTERVAL 90 DAY;
END$$

-- EVENT 2
CREATE EVENT ev_expired_memberships_check
ON SCHEDULE EVERY 1 DAY
DO
BEGIN
    INSERT INTO tbl_logs (log_type, message)
    SELECT 'EXPIRED', CONCAT('Membership for member ', member_id, ' expired today.')
    FROM tbl_member_membership
    WHERE end_date = CURDATE();
END$$

DELIMITER ;