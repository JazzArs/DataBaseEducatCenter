
-- INSERT INTO account (login, password_hash, role)
-- VALUES ('student201', 'hash', 'STUDENT')
-- RETURNING user_id;
-- select * from account;
-- INSERT INTO student (user_id, fio, group_id, phone, email)
-- VALUES (301, 'Лабанов Семён Семёнович', 1, '+79999999999', 'krutoy@mail.local');

-- update student
-- set fio = 'Лабанов Семен'
-- where user_id = 101;

DELETE FROM audit_log
WHERE log_id = 1;

select * from audit_log;


