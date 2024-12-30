// CONSULTA 21
MATCH (country:Country {name: 'Ethiopia'})
WITH
  country,
  datetime("2013-01-01T00:00:00.000+0000").year AS endDateYear,
  datetime("2013-01-01T00:00:00.000+0000").month AS endDateMonth
MATCH
  (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)
OPTIONAL MATCH
  (zombie)<-[:HAS_CREATOR]-(message:Message)
WHERE datetime(zombie.creationDate)  < datetime("2013-01-01T00:00:00.000+0000")
  AND datetime(message.creationDate) < datetime("2013-01-01T00:00:00.000+0000")
WITH
  country,
  zombie,
  endDateYear,
  endDateMonth,
  datetime(zombie.creationDate).year  AS zombieCreationYear,
  datetime(zombie.creationDate).month AS zombieCreationMonth,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * (endDateYear  - zombieCreationYear )
     + (endDateMonth - zombieCreationMonth)
     + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
OPTIONAL MATCH
  (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerZombie:Person)
WHERE likerZombie IN zombies
WITH
  zombie,
  count(likerZombie) AS zombieLikeCount
OPTIONAL MATCH
  (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerPerson:Person)
WHERE datetime(likerPerson.creationDate) < datetime("2013-01-01T00:00:00.000+0000")
WITH
  zombie,
  zombieLikeCount,
  count(likerPerson) AS totalLikeCount
RETURN
  zombie.id,
  zombieLikeCount,
  totalLikeCount,
  CASE totalLikeCount
    WHEN 0 THEN 0.0
    ELSE zombieLikeCount / toFloat(totalLikeCount)
  END AS zombieScore
ORDER BY
  zombieScore DESC,
  zombie.id ASC
LIMIT 100;


// ESTRATEGIA DE OPTIMIZACIÓN: MATERIALIZACIÓN
MATCH (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)<-[:HAS_CREATOR]-(message:Message)
CREATE (message)-[:BI21_message_person]->(zombie);


// CONSULTA OPTIMIZADA
MATCH (country:Country {name: 'Ethiopia'})
MATCH
  (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)
OPTIONAL MATCH
  (zombie)<-[:BI21_message_person]-(message:Message)
WHERE datetime(zombie.creationDate)  < datetime("2013-01-01T00:00:00.000+0000")
  AND datetime(message.creationDate) < datetime("2013-01-01T00:00:00.000+0000")
WITH
  country,
  zombie,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * (datetime("2013-01-01T00:00:00.000+0000").year  - datetime(zombie.creationDate).year)
     + (datetime("2013-01-01T00:00:00.000+0000").month - datetime(zombie.creationDate).month)
     + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
OPTIONAL MATCH
  (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerZombie:Person)
WHERE likerZombie IN zombies
WITH
  zombie,
  count(likerZombie) AS zombieLikeCount
OPTIONAL MATCH
  (zombie)<-[:HAS_CREATOR]-(message:Message)<-[:LIKES]-(likerPerson:Person)
WHERE datetime(likerPerson.creationDate) < datetime("2013-01-01T00:00:00.000+0000")
WITH
  zombie,
  zombieLikeCount,
  count(likerPerson) AS totalLikeCount,
  CASE count(likerPerson) 
    WHEN 0 THEN 0.0
    ELSE zombieLikeCount / toFloat(count(likerPerson))
  END AS zombieScore
ORDER BY
  zombieScore DESC,
  zombie.id ASC
LIMIT 100
RETURN
  zombie.id,
  zombieLikeCount,
  totalLikeCount,
  zombieScore 