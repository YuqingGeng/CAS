#SET SQL_SAFE_UPDATES = 0 # 更改模式便于update表格内容

#更改主键
#ALTER TABLE rel_org_tech MODIFY tech_id INT NOT NULL;
#ALTER TABLE `rel_org_tech` DROP PRIMARY KEY;
#ALTER TABLE `rel_org_tech` ADD PRIMARY KEY ( `tech_id` ) 

#查看主键是否更改成功
#desc rel_org_tech; 

#还原id的自增属性
#UPDATE rel_org_tech SET id = 0 WHERE id IS NULL
#ALTER TABLE rel_org_tech MODIFY id INT NOT NULL;
#alter table rel_org_tech ADD KEY test(id);
#ALTER TABLE rel_org_tech MODIFY id INT auto_increment

#对于每个新加入的文章，匹配tech_id与org_id
SET sql_safe_updates = 0;

INSERT INTO cra_organization(name_en)
SELECT ("Southampton University") FROM dual 
WHERE NOT EXISTS (SELECT*FROM cra_organization WHERE name_en="Southampton University");# #若不存在则添加
SET @tech_id1=(select MAX(id) from cra_organization); #设置最新的org_id
#SET @tech_id1=MAX(id);

INSERT INTO cra_technique_article (title) SELECT ("Test") FROM dual 
WHERE NOT EXISTS (SELECT * FROM cra_technique_article WHERE title ="Test");
#SELECT MAX(id) from cra_technique_article as org_id;
SET @org_id1=(select MAX(id) from cra_technique_article); #设置最新的tech_id

INSERT INTO rel_org_tech(tech_id, org_id, create_at) SELECT@tech_id1, @org_id1, sysdate() from dual
WHERE NOT EXISTS (SELECT * FROM rel_org_tech WHERE tech_id = @tech_id1 and org_id = @org_id1);
# ON DUPLICATE KEY UPDATE(key=MUL) update_at = sysdate();
#SELECT tech_id1, org_id1 from dual
#WHERE NOT EXISTS (SELECT * FROM rel_org_tech WHERE tech_id = tech_id1);


UPDATE cra_organization SET update_at = SYSDATE() WHERE name_en = "Southampton University"; # 不论是否存在更新update_at为当前时间

UPDATE rel_org_tech SET update_at = SYSDATE() WHERE org_id =@org_id1;

