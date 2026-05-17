USE kerry_mes;

CREATE TABLE machines (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type ENUM('pump','mixer','spray_dryer','compressor') NOT NULL,
    location VARCHAR(100),
    status ENUM('running','idle','maintenance','fault') DEFAULT 'running',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE maintenance_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    machine_id VARCHAR(20) NOT NULL,
    maintenance_type ENUM('preventive','corrective','emergency') NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    technician VARCHAR(100),
    notes TEXT,
    mtbf_hours FLOAT,
    mttr_hours FLOAT,
    FOREIGN KEY (machine_id) REFERENCES machines(id)
);

CREATE TABLE shift_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    machine_id VARCHAR(20) NOT NULL,
    shift_date DATE NOT NULL,
    shift_type ENUM('morning','afternoon','night') NOT NULL,
    operator_name VARCHAR(100),
    machine_status ENUM('running','idle','maintenance','fault') DEFAULT 'running',
    notes TEXT,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (machine_id) REFERENCES machines(id)
);

-- Seed: 4 machines
INSERT INTO machines (id, name, type, location, status) VALUES
('pump_01',       'Feed Pump #1',       'pump',        'Line A - Wet Section',  'running'),
('mixer_01',      'Ingredient Mixer #1','mixer',        'Line A - Mix Section',  'running'),
('spray_dryer_01','Spray Dryer #1',     'spray_dryer',  'Line A - Dry Section',  'running'),
('compressor_01', 'Air Compressor #1',  'compressor',   'Utility Room',          'running');

-- Seed: historical maintenance records
INSERT INTO maintenance_records (machine_id, maintenance_type, start_time, end_time, technician, notes, mtbf_hours, mttr_hours) VALUES
('pump_01',       'preventive', '2026-05-01 08:00:00', '2026-05-01 10:00:00', 'Ahmad Fauzi',   'Routine bearing inspection',        720.0, 2.0),
('mixer_01',      'corrective', '2026-05-05 14:00:00', '2026-05-05 17:30:00', 'Budi Santoso',  'Replaced worn mixing blade',         480.0, 3.5),
('spray_dryer_01','preventive', '2026-05-10 06:00:00', '2026-05-10 09:00:00', 'Citra Dewi',    'Nozzle cleaning & pressure check',   600.0, 3.0),
('compressor_01', 'emergency',  '2026-05-12 22:00:00', '2026-05-13 02:00:00', 'Deni Kurniawan','Overheating — coolant refill',        360.0, 4.0);

-- Seed: recent shift logs
INSERT INTO shift_logs (machine_id, shift_date, shift_type, operator_name, machine_status) VALUES
('pump_01',       '2026-05-16', 'morning',   'Eko Prasetyo',  'running'),
('pump_01',       '2026-05-16', 'afternoon', 'Fajar Hidayat', 'running'),
('mixer_01',      '2026-05-16', 'morning',   'Gita Rahayu',   'running'),
('spray_dryer_01','2026-05-16', 'morning',   'Hendra Wijaya', 'idle'),
('compressor_01', '2026-05-16', 'night',     'Irfan Maulana', 'running');
