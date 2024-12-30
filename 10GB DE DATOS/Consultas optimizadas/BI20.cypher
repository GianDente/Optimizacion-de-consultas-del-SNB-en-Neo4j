// CONSULTA 20
UNWIND ['Writer', 'Single', 'Country'] AS tagClassName
MATCH
  (tagClass:TagClass {name: tagClassName})<-[:IS_SUBCLASS_OF*0..]-(:TagClass)<-[:HAS_TYPE]-(tag:Tag)<-[:HAS_TAG]-(message:Message)
RETURN
  tagClass.name,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  tagClass.name ASC
LIMIT 100; 


// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN
MATCH (tagClass:TagClass)<-[:IS_SUBCLASS_OF*0..]-(:TagClass)<-[:HAS_TYPE]-(tag:Tag)<-[:HAS_TAG]-(message:Message)
CREATE (message)-[:BI20_message_tagClass]->(tagClass);

// CONSULTA OPTIMIZADA
UNWIND ['Writer', 'Single', 'Country'] AS tagClassName
MATCH
  (tagClass:TagClass {name: tagClassName})<-[:BI20_message_tagClass]-(message:Message)
WITH count(DISTINCT message) AS messageCount, tagClass
ORDER BY
  messageCount DESC,
  tagClass.name ASC
LIMIT 100
RETURN
  tagClass.name,
  messageCount