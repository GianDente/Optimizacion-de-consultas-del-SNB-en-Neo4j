// CONSULTA 23
MATCH
  (home:Country {name: 'Egypt'})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(:Person)<-[:HAS_CREATOR]-(message:Message)-[:IS_LOCATED_IN]->(destination:Country)
WHERE home <> destination
WITH
  message,
  destination,
  datetime(message.creationDate).month AS month
RETURN
  count(message) AS messageCount,
  destination.name,
  month
ORDER BY
  messageCount DESC,
  destination.name ASC,
  month ASC
LIMIT 100; 


// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN + ÍNDICES

// MATERIALIZACIÓN
MATCH
  (home:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_CREATOR]-(message:Message)
CREATE (home)<-[:BI23_message_country]-(message);

// ÍNDICES
CREATE LOOKUP INDEX index_343aff4e FOR (n) ON EACH labels(n);
CREATE LOOKUP INDEX index_f7700477 FOR ()-[r]-() ON EACH type(r);


// CONSULTA OPTIMIZADA
MATCH
  (home:Country {name: 'Egypt'})<-[:BI23_message_country]-(message:Message)-[:IS_LOCATED_IN]->(destination:Country)
WHERE home <> destination
WITH
  count(message) AS messageCount,
  destination,
  datetime(message.creationDate).month AS month
ORDER BY
  messageCount DESC,
  destination.name ASC,
  month ASC
LIMIT 100
RETURN
  messageCount,
  destination.name,
  month 