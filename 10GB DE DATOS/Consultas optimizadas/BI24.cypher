// CONSULTA 24
MATCH (:TagClass {name: 'Single'})<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(message:Message)
WITH DISTINCT message
MATCH (message)-[:IS_LOCATED_IN]->(:Country)-[:IS_PART_OF]->(continent:Continent)
OPTIONAL MATCH (message)<-[like:LIKES]-(:Person)
WITH
  message,
  datetime(message.creationDate).year AS year,
  datetime(message.creationDate).month AS month,
  like,
  continent
RETURN
  count(DISTINCT message) AS messageCount,
  count(like) AS likeCount,
  year,
  month,
  continent.name
ORDER BY
  year ASC,
  month ASC,
  continent.name DESC
LIMIT 100;



// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN
MATCH (tagClass:TagClass)<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(message:Message)
CREATE (tagClass)<-[:BI24_tagClass_message]-(message);

MATCH (message:Message)-[:IS_LOCATED_IN]->(:Country)-[:IS_PART_OF]->(continent:Continent)
CREATE (message)-[:BI24_message_continent]->(continent);


// CONSULTA OPTIMIZADA
MATCH (tagClass:TagClass {name: 'Single'})<-[:BI24_tagClass_message]-(message:Message)
WITH DISTINCT message
MATCH (message)-[:BI24_message_continent]->(continent:Continent)
OPTIONAL MATCH (message)<-[like:LIKES]-(:Person)
WITH
  datetime(message.creationDate).year AS year,
  datetime(message.creationDate).month AS month,
  count(DISTINCT message) AS messageCount,
  count(like) AS likeCount,
  continent
ORDER BY
  year ASC,
  month ASC,
  continent.name DESC
LIMIT 100
RETURN
  messageCount,
  likeCount,
  year,
  month,
  continent.name