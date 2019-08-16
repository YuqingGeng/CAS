SET sql_safe_updates = 0;
    SET @key_id1=(select id from technique_words WHERE keyword = "deep sea robot");
    SET @tech_id1=(select MAX(id) from cra_technique_article); 
    INSERT INTO rel_article_keywords(article_id, keyword_id, create_at, type) SELECT@tech_id1, @key_id1, sysdate(), "1" from dual
    WHERE NOT EXISTS (SELECT * FROM rel_article_keywords WHERE article_id = @tech_id1 and keyword_id = @key_id1);
    UPDATE rel_article_keywords SET update_at = SYSDATE() WHERE article_id =@tech_id1;