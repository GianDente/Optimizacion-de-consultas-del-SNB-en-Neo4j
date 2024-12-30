// CONSULTA 7
MATCH (tag:Tag {name: 'Arnold_Schwarzenegger'})
MATCH (tag)<-[:HAS_TAG]-(message1:Message)-[:HAS_CREATOR]->(person1:Person)
MATCH (tag)<-[:HAS_TAG]-(message2:Message)-[:HAS_CREATOR]->(person1)
OPTIONAL MATCH (message2)<-[:LIKES]-(person2:Person)
OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message3:Message)<-[like:LIKES]-(p3:Person)
RETURN
  person1.id,
  count(DISTINCT like) AS authorityScore
ORDER BY
  authorityScore DESC,
  person1.id ASC
LIMIT 100;


// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN
MATCH (message1:Message)-[:HAS_CREATOR]->(person1:Person)
CREATE (message1)-[:BI7_message1_person1]->(person1);

MATCH (person2:Person)<-[:HAS_CREATOR]-(message3:Message)<-[like:LIKES]-(p3:Person)
CREATE (p3)-[:BI7_person_person]->(person2);


// CONSULTA OPTIMIZADA
MATCH (tag:Tag {name: 'Arnold_Schwarzenegger'})
MATCH (tag)<-[:HAS_TAG]-(message1:Message)-[:BI7_message1_person1]->(person1:Person)
OPTIONAL MATCH (message1)<-[:LIKES]-(person2:Person)
OPTIONAL MATCH (person2)<-[like:BI7_person_person]-(p3:Person)
WITH count(DISTINCT like) AS authorityScore, person1
ORDER BY
  authorityScore DESC,
  person1.id ASC
LIMIT 100
RETURN
  person1.id,
  authorityScore