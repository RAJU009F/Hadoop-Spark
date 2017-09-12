DROP table IF EXISTS `employee` ;
CREATE TABLE `employee` ( 
   `id` INT NOT NULL PRIMARY KEY, 
   `name` VARCHAR(20), 
   `deg` VARCHAR(20),
   `salary` INT,
   `dept` VARCHAR(10));
   
-- INSERT 
LOCK TABLES `employee` WRITE;
INSERT INTO `employee` values(1201, 'gopal',     'manager', 50000, 'TP');   
INSERT INTO `employee` values(1202, 'manisha',   'preader', 50000, 'TP');   

INSERT INTO `employee` values(1203, 'kalil',     'php dev', 30000,'AC');   
INSERT INTO `employee` values(1204, 'prasanth',  'php dev', 30000, 'AC');   
INSERT INTO `employee` values(1205, 'kranthi',   'admin',   20000, 'TP');   
INSERT INTO `employee` values(1206, 'satish p',  'grp des', 20000, 'GR');   
UNLOCK TABLES;
