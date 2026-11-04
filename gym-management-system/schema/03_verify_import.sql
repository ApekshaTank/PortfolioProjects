USE gym_management_system;

-- Check total records in all tables

SELECT 'tbl_members' AS table_name,
       COUNT(*) AS total_records
FROM tbl_members

UNION ALL

SELECT 'tbl_memberships',
       COUNT(*)
FROM tbl_memberships

UNION ALL

SELECT 'tbl_member_membership',
       COUNT(*)
FROM tbl_member_membership

UNION ALL

SELECT 'tbl_trainers',
       COUNT(*)
FROM tbl_trainers

UNION ALL

SELECT 'tbl_classes',
       COUNT(*)
FROM tbl_classes

UNION ALL

SELECT 'tbl_class_schedule',
       COUNT(*)
FROM tbl_class_schedule

UNION ALL

SELECT 'tbl_equipment',
       COUNT(*)
FROM tbl_equipment

UNION ALL

SELECT 'tbl_equipment_maintenance',
       COUNT(*)
FROM tbl_equipment_maintenance;

-- VERIFY FOREIGN KEY INTEGRITY

-- Check membership records without valid members

SELECT COUNT(*) AS invalid_members
FROM tbl_member_membership mm
LEFT JOIN tbl_members m
    ON mm.member_id = m.member_id
WHERE m.member_id IS NULL;


-- Check membership records without valid plans

SELECT COUNT(*) AS invalid_memberships
FROM tbl_member_membership mm
LEFT JOIN tbl_memberships ms
    ON mm.membership_id = ms.membership_id
WHERE ms.membership_id IS NULL;


-- Check classes without valid trainers

SELECT COUNT(*) AS invalid_trainers
FROM tbl_classes c
LEFT JOIN tbl_trainers t
    ON c.trainer_id = t.trainer_id
WHERE c.trainer_id IS NOT NULL
AND t.trainer_id IS NULL;


-- Check schedules without valid classes

SELECT COUNT(*) AS invalid_classes
FROM tbl_class_schedule cs
LEFT JOIN tbl_classes c
    ON cs.class_id = c.class_id
WHERE c.class_id IS NULL;


-- Check maintenance without valid equipment

SELECT COUNT(*) AS invalid_equipment
FROM tbl_equipment_maintenance em
LEFT JOIN tbl_equipment e
    ON em.equipment_id = e.equipment_id
WHERE e.equipment_id IS NULL;