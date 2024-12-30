// CONSULTA 8
MATCH
  (tag:Tag {name: 'Genghis_Khan'})<-[:HAS_TAG]-(message:Message),
  (message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
WHERE NOT (comment)-[:HAS_TAG]->(tag)
RETURN
  relatedTag.name,
  count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  relatedTag.name ASC
LIMIT 100; 


// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN
MATCH (tag:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)
CREATE (tag)<-[:BI8_comment_tag]-(comment);


// CONSULTA OPTIMIZADA
MATCH
  (tag:Tag {name: 'Genghis_Khan'})<-[:BI8_comment_tag]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
WHERE NOT (comment)-[:HAS_TAG]->(tag)
WITH relatedTag, count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  relatedTag.name ASC
LIMIT 100
RETURN
  relatedTag.name,
  count 
