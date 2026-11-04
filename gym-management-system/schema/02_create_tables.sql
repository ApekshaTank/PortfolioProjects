-- GYM MANAGEMENT SYSTEM
-- STEP 2: CREATE ALL 8 TABLES

USE gym_management;

-- 1. MEMBERS TABLE
CREATE TABLE tbl_members (
    member_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone_number VARCHAR(20) NOT NULL,
    address VARCHAR(255) NOT NULL,
    date_of_birth DATE NOT NULL,
    join_date DATE NOT NULL
);


-- 2. MEMBERSHIPS TABLE
CREATE TABLE tbl_memberships (
    membership_id INT PRIMARY KEY AUTO_INCREMENT,
    membership_type VARCHAR(50) NOT NULL,
    description VARCHAR(255),
    duration_in_months INT NOT NULL,
    price DECIMAL(10,2) NOT NULL
);


-- 3. MEMBER MEMBERSHIP TABLE
CREATE TABLE tbl_member_membership (
    member_membership_id INT PRIMARY KEY AUTO_INCREMENT,
    member_id INT NOT NULL,
    membership_id INT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,

    CONSTRAINT fk_mm_member
        FOREIGN KEY (member_id)
        REFERENCES tbl_members(member_id),

    CONSTRAINT fk_mm_membership
        FOREIGN KEY (membership_id)
        REFERENCES tbl_memberships(membership_id)
);


-- 4. TRAINERS TABLE
CREATE TABLE tbl_trainers (
    trainer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone_number VARCHAR(20) NOT NULL,
    specialty VARCHAR(100) NOT NULL,
    hire_date DATE NOT NULL
);
select * from tbl_trainers;

-- 5. CLASSES TABLE
CREATE TABLE tbl_classes (
    class_id INT PRIMARY KEY AUTO_INCREMENT,
    class_name VARCHAR(100) NOT NULL,
    description VARCHAR(255),
    trainer_id INT,

    CONSTRAINT fk_class_trainer
        FOREIGN KEY (trainer_id)
        REFERENCES tbl_trainers(trainer_id)
);


-- 6. CLASS SCHEDULE TABLE
CREATE TABLE tbl_class_schedule (
    schedule_id INT PRIMARY KEY AUTO_INCREMENT,
    class_id INT NOT NULL,
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,

    CONSTRAINT fk_schedule_class
        FOREIGN KEY (class_id)
        REFERENCES tbl_classes(class_id)
);


-- 7. EQUIPMENT TABLE
CREATE TABLE tbl_equipment (
    equipment_id INT PRIMARY KEY AUTO_INCREMENT,
    equipment_name VARCHAR(100) NOT NULL,
    description VARCHAR(255),
    purchase_date DATE NOT NULL,
    last_maintenance_date DATE NULL
);


-- 8. EQUIPMENT MAINTENANCE TABLE
CREATE TABLE tbl_equipment_maintenance (
    maintenance_id INT PRIMARY KEY AUTO_INCREMENT,
    equipment_id INT NOT NULL,
    maintenance_date DATE NOT NULL,
    description VARCHAR(255),

    CONSTRAINT fk_maintenance_equipment
        FOREIGN KEY (equipment_id)
        REFERENCES tbl_equipment(equipment_id)
);

-- data is inserted using table data import wizard