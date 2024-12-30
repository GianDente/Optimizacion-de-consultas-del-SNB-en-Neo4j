// CONSULTA 3
WITH 
  2010 AS year1, 
  10 AS month1, 
  2010 + toInteger(10 / 12.0) AS year2, 
  10 % 12 + 1 AS month2
MATCH (tag:Tag)
OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag)
WHERE datetime(message1.creationDate).year = year1
  AND datetime(message1.creationDate).month = month1
WITH year2, month2, tag, count(message1) AS countMonth1
OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag)
WHERE datetime(message2.creationDate).year  = year2
  AND datetime(message2.creationDate).month = month2
WITH 
  tag, countMonth1, count(message2) AS countMonth2
RETURN 
  tag.name, 
  countMonth1, 
  countMonth2, 
  abs(countMonth1 - countMonth2) AS diff
ORDER BY 
  diff DESC, 
  tag.name ASC
LIMIT 100;


// ESTRATEGIA DE OPTIMIZACIÓN: REESSCRITURA

// CONSULTA OPTIMIZADA
WITH
  2010 AS year1,
  10 AS month1,
  2010 + toInteger(10 / 12.0) AS year2,
  10 % 12 + 1 AS month2
MATCH (tag:Tag)
OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message:Message)
WITH 
  tag,
  SUM(CASE WHEN datetime(message.creationDate).year = year1 AND datetime(message.creationDate).month = month1 
      THEN 1 ELSE 0 END) AS countMsg1,
  SUM(CASE WHEN datetime(message.creationDate).year = year2 AND datetime(message.creationDate).month = month2 
      THEN 1 ELSE 0 END) AS countMsg
RETURN
  tag.name,
  countMsg1,
  countMsg,
  abs(countMsg1 - countMsg) AS diff
ORDER BY
  diff DESC,
  tag.name ASC
LIMIT 100; 