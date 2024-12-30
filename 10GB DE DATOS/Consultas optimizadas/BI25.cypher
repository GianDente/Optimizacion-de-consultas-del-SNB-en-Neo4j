// CONSULTA 25
MATCH path=allShortestPaths((p1:Person {id: "19791209303405"})-[:KNOWS*]-(p2:Person {id: "19791209308983"}))
UNWIND relationships(path) AS k
WITH path, startNode(k) AS pA, endNode(k) AS pB, 0 AS relationshipWeights
// case 1, A to B
OPTIONAL MATCH 
  (pA)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(pB), 
  (post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE datetime(forum.creationDate) >= datetime("2010-10-31T23:00:00.000+0000") AND datetime(forum.creationDate) <= datetime("2010-11-30T23:00:00.000+0000")
WITH path, pA, pB, relationshipWeights, count(c) AS countC
WITH path, pA, pB, relationshipWeights + countC * 1.0 AS relationshipWeights
// case 2, A to B
OPTIONAL MATCH 
  (pA)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)-[:HAS_CREATOR]->(pB), 
  (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE datetime(forum.creationDate) >= datetime("2010-10-31T23:00:00.000+0000") AND datetime(forum.creationDate) <= datetime("2010-11-30T23:00:00.000+0000")
WITH path, pA, pB, relationshipWeights, count(c1) AS countC1
WITH path, pA, pB, relationshipWeights + countC1 * 0.5 AS relationshipWeights
// case 1, B to A
OPTIONAL MATCH 
  (pB)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(pA), 
  (post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE datetime(forum.creationDate) >= datetime("2010-10-31T23:00:00.000+0000") AND datetime(forum.creationDate) <= datetime("2010-11-30T23:00:00.000+0000")
WITH path, pA, pB, relationshipWeights, count(c) AS countC_BA
WITH path, pA, pB, relationshipWeights + countC_BA * 1.0 AS relationshipWeights
// case 2, B to A
OPTIONAL MATCH 
  (pB)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)-[:HAS_CREATOR]->(pA), 
  (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE datetime(forum.creationDate) >= datetime("2010-10-31T23:00:00.000+0000") AND datetime(forum.creationDate) <= datetime("2010-11-30T23:00:00.000+0000")
WITH path, pA, pB, relationshipWeights, count(c1) AS countC1_BA
WITH path, pA, pB, relationshipWeights + countC1_BA * 0.5 AS relationshipWeights
WITH [person IN nodes(path) | person.id] AS personIds, sum(relationshipWeights) AS weight
RETURN personIds, weight
ORDER BY weight DESC, personIds ASC; 


// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN
MATCH (pA)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)
CREATE (pA)-[c:BI25_reply_post]->(post);

MATCH (pA)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)
CREATE (pA)-[c1:BI25_reply_comment]->(c2); 


// CONSULTA OPTIMIZADA
MATCH path=allShortestPaths((p1:Person {id: "19791209303405"})-[:KNOWS*]-(p2:Person {id: "19791209308983"}))
UNWIND relationships(path) AS k
WITH path, startNode(k) AS pA, endNode(k) AS pB, 0 AS relationshipWeights
// case 1, A to B
OPTIONAL MATCH
  (pA)-[c:BI25_reply_post]->(post:Post)-[:HAS_CREATOR]->(pB),
  (post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE datetime(forum.creationDate) >= datetime("2010-10-31T23:00:00.000+0000") AND datetime(forum.creationDate) <= datetime("2010-11-30T23:00:00.000+0000")
WITH path, pA, pB, relationshipWeights, count(c) AS countC
WITH path, pA, pB, relationshipWeights + countC * 1.0 AS relationshipWeights
// case 2, A to B
OPTIONAL MATCH
  (pA)-[c1:BI25_reply_comment]->(c2:Comment)-[:HAS_CREATOR]->(pB),
  (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE datetime(forum.creationDate) >= datetime("2010-10-31T23:00:00.000+0000") AND datetime(forum.creationDate) <= datetime("2010-11-30T23:00:00.000+0000")
WITH path, pA, pB, relationshipWeights, count(c1) AS countC1
WITH path, pA, pB, relationshipWeights + countC1 * 0.5 AS relationshipWeights
// case 1, B to A
OPTIONAL MATCH
  (pB)-[c:BI25_reply_post]->(post:Post)-[:HAS_CREATOR]->(pA),
  (post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE datetime(forum.creationDate) >= datetime("2010-10-31T23:00:00.000+0000") AND datetime(forum.creationDate) <= datetime("2010-11-30T23:00:00.000+0000")
WITH path, pA, pB, relationshipWeights, count(c) AS countC_BA
WITH path, pA, pB, relationshipWeights + countC_BA * 1.0 AS relationshipWeights
// case 2, B to A
OPTIONAL MATCH
  (pB)-[c1:BI25_reply_comment]->(c2:Comment)-[:HAS_CREATOR]->(pA),
  (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE datetime(forum.creationDate) >= datetime("2010-10-31T23:00:00.000+0000") AND datetime(forum.creationDate) <= datetime("2010-11-30T23:00:00.000+0000")
WITH path, pA, pB, relationshipWeights, count(c1) AS countC1_BA
WITH path, pA, pB, relationshipWeights + countC1_BA * 0.5 AS relationshipWeights
WITH
  [person IN nodes(path) | person.id] AS personIds, sum(relationshipWeights) AS weight
RETURN personIds, weight
ORDER BY weight DESC, personIds ASC