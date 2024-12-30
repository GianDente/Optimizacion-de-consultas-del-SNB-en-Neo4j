// CONSULTA 9
MATCH
  (forum:Forum)-[:HAS_MEMBER]->(person:Person)
WITH forum, count(person) AS members
WHERE members > 200
MATCH
  (forum)-[:CONTAINER_OF]->(post1:Post)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: 'BaseballPlayer'})
WITH forum, count(DISTINCT post1) AS count1
MATCH
  (forum)-[:CONTAINER_OF]->(post2:Post)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: 'ChristianBishop'})
WITH forum, count1, count(DISTINCT post2) AS count2
RETURN
  forum.id,
  count1,
  count2
ORDER BY
  abs(count2-count1) DESC,
  forum.id ASC
LIMIT 100;



// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN
MATCH (post:Post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)
CREATE (post)-[:BI9_post_tagClass]->(tagClass);

MATCH (forum:Forum)-[:HAS_MEMBER]->(person:Person)
CREATE (forum)-[:BI9_forum_person]->(person);


// CONSULTA OPTIMIZADA
MATCH
  (forum:Forum)-[:BI9_forum_person]->(person:Person)
WITH forum, count(person) AS members
WHERE members > 200
MATCH
  (forum)-[:CONTAINER_OF]->(post1:Post)-[:BI9_post_tagClass]->(tagClass:TagClass {name: 'BaseballPlayer'})
WITH forum, count(DISTINCT post1) AS count1
MATCH
  (forum)-[:CONTAINER_OF]->(post2:Post)-[:BI9_post_tagClass]->(tagClass:TagClass {name: 'ChristianBishop'})
WITH forum, count1, count(DISTINCT post2) AS count2
ORDER BY
  abs(count2-count1) DESC,
  forum.id ASC
LIMIT 100
RETURN
  forum.id,
  count1,
  count2