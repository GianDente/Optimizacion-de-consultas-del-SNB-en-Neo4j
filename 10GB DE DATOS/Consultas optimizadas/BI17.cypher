// CONSULTA 17
MATCH (country:Country {name: 'Spain'})
MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
MATCH (b:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
MATCH (c:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
MATCH (a)-[:KNOWS]-(b), (b)-[:KNOWS]-(c), (c)-[:KNOWS]-(a)
WHERE a.id < b.id
  AND b.id < c.id
RETURN count(*) AS count;


// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN
MATCH (person:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country:Country)
CREATE (person)-[:BI17_person_country]->(country);


// CONSULTA OPTIMIZADA
MATCH (country:Country {name: 'Spain'})
MATCH (a:Person)-[:BI17_person_country]->(country)
MATCH (b:Person)-[:BI17_person_country]->(country)
MATCH (c:Person)-[:BI17_person_country]->(country)
MATCH (a)-[:KNOWS]-(b), (b)-[:KNOWS]-(c), (c)-[:KNOWS]-(a)
WHERE a.id < b.id
  AND b.id < c.id
RETURN count(*) AS count