// Carga de datos 10gb
// Primero debemos colocar todos los csv en la carpeta import de la bd en Neo4j

// Static //

// Entidad Place
LOAD CSV WITH HEADERS FROM 'file:///place.csv' AS row FIELDTERMINATOR '|'
MERGE(p:Place {id: row.id, name: row.name, url: row.url, type: row.type})

// Entidad Country. Para nodos de tipo country. Establecer etiqueta
MATCH (p:Place {type: 'country'})
SET p:Country

// Entidad City. Para nodos de tipo city. Establecer etiqueta
MATCH (p:Place {type: 'city'})
SET p:City

// Entidad Continent. Para nodos de tipo continent. Establecer etiqueta
MATCH (p:Place {type: 'continent'})
SET p:Continent

// Entidad Organisation
LOAD CSV WITH HEADERS FROM 'file:///organisation.csv' AS row FIELDTERMINATOR '|'
MERGE (o:Organisation {id: row.id, type: row.type, name: row.name, url: row.url})

// Entidad University. Para nodos de tipo university. Establecer etiqueta
MATCH (o:Organisation {type: 'university'})
SET o:University

// Entidad Company. Para nodos de tipo company. Establecer etiqueta
MATCH (o:Organisation {type: 'company'})
SET o:Company

// Entidad Tag
LOAD CSV WITH HEADERS FROM 'file:///tag.csv' AS row FIELDTERMINATOR '|'
MERGE (t:Tag {id: row.id, name: row.name, url: row.url})

// Entidad TagClass
LOAD CSV WITH HEADERS FROM 'file:///tagclass.csv' AS row FIELDTERMINATOR '|'
MERGE (t:TagClass {id: row.id, name: row.name, url: row.url})

// Relacion organisation-isLocatedIn->place
LOAD CSV WITH HEADERS FROM 'file:///organisation_isLocatedIn_place.csv' AS row FIELDTERMINATOR '|'
MATCH (o:Organisation {id: row.Organisation_id})
MATCH (p:Place {id: row.Place_id})
CREATE (o)-[:IS_LOCATED_IN]->(p);

// Relacion place-isPartOf->place
LOAD CSV WITH HEADERS FROM 'file:///place_isPartOf_place.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Place {id: row.Place2_id})
MATCH (pp:Place {id: row.Place_id})
MERGE (p)-[:IS_PART_OF]->(pp);

// Relacion tag-hasType->tagclass
LOAD CSV WITH HEADERS FROM 'file:///tag_hasType_tagclass.csv' AS row FIELDTERMINATOR '|'
MATCH (t:Tag {id: row.Tag_id})
MATCH (tc:TagClass {id: row.TagClass_id})
MERGE (t)-[:HAS_TYPE]->(tc);

// Relacion tagClass-isSubClassOf->tagclass
LOAD CSV WITH HEADERS FROM 'file:///tagClass_isSubClassOf_tagclass.csv' AS row FIELDTERMINATOR '|'
MATCH (tc:TagClass {id: row.TagClass_id})
MATCH (tcc:TagClass {id: row.TagClass2_id})
MERGE (tc)-[:IS_SUBCLASS_OF]->(tcc);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Dynamic //

// Entidad Comment
LOAD CSV WITH HEADERS FROM 'file:///comment.csv' AS row FIELDTERMINATOR '|'
MERGE(c:Comment {id: row.id, creationDate: datetime(row.creationDate), locationIP: row.locationIP, browserUsed: row.browserUsed,
                 content:row.content, length:toInteger(row.length)})

// Actualizar tipos de datos, porque se cargo con import (el comando los coloca todos en string)
MATCH (c:Comment)
SET c.creationDate = datetime(c.creationDate),
    c.length = toInteger(c.length),
    c.id = toInteger(c.id)

// Entidad Forum
LOAD CSV WITH HEADERS FROM 'file:///forum.csv' AS row FIELDTERMINATOR '|'
MERGE(f:Forum {id: row.id, title: row.title, creationDate: datetime(row.creationDate)})

// Entidad Post
LOAD CSV WITH HEADERS FROM 'file:///post.csv' AS row FIELDTERMINATOR '|'
MERGE(p:Post {id: row.id, imageFile: row.imageFile, creationDate: datetime(row.creationDate), locationIP: row.locationIP, 
              browserUsed: row.browserUsed, language: row.language, content: row.content, length: toInteger(row.length)})

// Actualizar tipos de datos, porque se cargo con import (el comando los coloca todos en string)
MATCH (p:Post)
SET p.creationDate = datetime(p.creationDate),
    p.id = toInteger(p.id),
    p.length = toInteger(p.length)

// Entidad Message. Establecer etiqueta
MATCH (c:Comment)
SET c:Message

MATCH (p:Post)
SET p:Message

// Entidad Person
LOAD CSV WITH HEADERS FROM 'file:///person.csv' AS row FIELDTERMINATOR '|'
MERGE(p:Person {id: row.id, firstName: row.firstName, lastName: row.lastName, gender: row.gender, birthday: date(row.birthday),
                creationDate: datetime(row.creationDate), locationIP: row.locationIP, browserUsed: row.browserUsed})

LOAD CSV WITH HEADERS FROM 'file:///person_email_emailaddress.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: row.Person_id})
SET p.email = row.email

// Para una carga mas rapida:
:auto LOAD CSV WITH HEADERS FROM 'file:///person_email_emailaddress.csv' AS row FIELDTERMINATOR '|'
CALL {
    WITH row
    MATCH (p:Person {id: row.Person_id})
    SET p.email = row.email
} IN TRANSACTIONS OF 10000 ROWS;

////////////////////////////// Relaciones //////////////////////////////////////

// Indices para la carga rapida de las relaciones
CREATE INDEX comment_id_index FOR (c:Comment) ON (c.id);
CREATE INDEX person_id_index FOR (p:Person) ON (p.id);
CREATE INDEX tag_id_index FOR (t:Tag) ON (t.id);
CREATE INDEX organisation_id_index FOR (o:Organisation) ON (o.id);
CREATE INDEX post_id_index FOR (p:Post) ON (p.id);
CREATE INDEX forum_id_index FOR (f:Forum) ON (f.id);
CREATE INDEX place_id_index FOR (p:Place) ON (p.id);

// Relacion comment-hasCreator->person
LOAD CSV WITH HEADERS FROM 'file:///comment_hasCreator_person.csv' AS row FIELDTERMINATOR '|'
MATCH (c:Comment {id: row.Comment_id})
MATCH (p:Person {id: row.Person_id})
MERGE (c)-[:HAS_CREATOR]->(p);

// o 
:auto LOAD CSV WITH HEADERS FROM 'file:///comment_hasCreator_person.csv' AS row FIELDTERMINATOR '|'
CALL {
    WITH row
    MATCH (c:Comment {id: row.Comment_id})
    MATCH (p:Person {id: row.Person_id})
    MERGE (c)-[:HAS_CREATOR]->(p)
} IN TRANSACTIONS OF 10000 ROWS;


// Relacion comment-hasTag->tag
LOAD CSV WITH HEADERS FROM 'file:///comment_hasTag_tag.csv' AS row FIELDTERMINATOR '|'
MATCH (c:Comment {id: row.Comment_id})
MATCH (t:Tag {id: row.Tag_id})
MERGE (c)-[:HAS_TAG]->(t);

// Relacion comment-isLocatedIn->place
LOAD CSV WITH HEADERS FROM 'file:///comment_isLocatedIn_place.csv' AS row FIELDTERMINATOR '|'
MATCH (c:Comment {id: row.Comment_id})
MATCH (p:Place {id: row.Place_id})
MERGE (c)-[:IS_LOCATED_IN]->(p);

// Relacion comment-replyOf->comment
LOAD CSV WITH HEADERS FROM 'file:///comment_replyOf_comment.csv' AS row FIELDTERMINATOR '|'
MATCH (c:Comment {id: row.Comment_id})
MATCH (cc:Comment {id: row.Comment2_id})
MERGE (c)-[:REPLY_OF]->(cc);

// Relacion comment-replyOf->post
LOAD CSV WITH HEADERS FROM 'file:///comment_replyOf_post.csv' AS row FIELDTERMINATOR '|'
MATCH (c:Comment {id: row.Comment_id})
MATCH (p:Post {id: row.Post_id})
CREATE (c)-[:REPLY_OF]->(p);

// Relacion forum-containerOf->post
LOAD CSV WITH HEADERS FROM 'file:///forum_containerOf_post.csv' AS row FIELDTERMINATOR '|'
MATCH (f:Forum {id: row.Forum_id})
MATCH (p:Post {id: row.Post_id})
MERGE (f)-[:CONTAINER_OF]->(p);

// Relacion forum-hasMember->person
LOAD CSV WITH HEADERS FROM 'file:///forum_hasMember_person.csv' AS row FIELDTERMINATOR '|'
MATCH (f:Forum {id: row.Forum_id})
MATCH (p:Person {id: row.Person_id})
MERGE (f)-[fp:HAS_MEMBER{joinDate: datetime(row.joinDate)}]->(p);

// Relacion forum-hasModerator->person
LOAD CSV WITH HEADERS FROM 'file:///forum_hasModerator_person.csv' AS row FIELDTERMINATOR '|'
MATCH (f:Forum {id: row.Forum_id})
MATCH (p:Person {id: row.Person_id})
MERGE (f)-[:HAS_MODERATOR]->(p);

// Relacion forum-hasTag->tag
LOAD CSV WITH HEADERS FROM 'file:///forum_hasTag_tag.csv' AS row FIELDTERMINATOR '|'
MATCH (f:Forum {id: row.Forum_id})
MATCH (t:Tag {id: row.Tag_id})
CREATE (f)-[:HAS_TAG]->(t);

// Relacion person-hasInterest->tag
LOAD CSV WITH HEADERS FROM 'file:///person_hasInterest_tag.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: row.Person_id})
MATCH (t:Tag {id: row.Tag_id})
MERGE (p)-[:HAS_INTEREST]->(t);

// Relacion person-isLocatedIn->place
LOAD CSV WITH HEADERS FROM 'file:///person_isLocatedIn_place.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: row.Person_id})
MATCH (pp:Place {id: row.Place_id})
CREATE (p)-[:IS_LOCATED_IN]->(pp);

// Relacion person-knows->person
LOAD CSV WITH HEADERS FROM 'file:///person_knows_person.csv' AS row FIELDTERMINATOR '|'
MATCH (p1:Person {id: row.Person_id})
MATCH (p2:Person {id: row.Person2_id})
MERGE (p1)-[pp:KNOWS {creationDate: datetime(row.creationDate)}]->(p2);

// Relacion person-likes->comment
LOAD CSV WITH HEADERS FROM 'file:///person_likes_comment.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: row.Person_id})
MATCH (c:Comment {id: row.Comment_id})
MERGE (p)-[pc:LIKES {creationDate: datetime(row.creationDate)}]->(c);

// Relacion person-likes->post
LOAD CSV WITH HEADERS FROM 'file:///person_likes_post.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: row.Person_id})
MATCH (pp:Post {id: row.Post_id})
CREATE (p)-[ppp:LIKES {creationDate: datetime(row.creationDate)}]->(pp); 

// Relacion person-speaks->language y Entidad Language (no hay un archivo de excel que solo 
// contenga la entidad Lenguage pero se puede crear)
LOAD CSV WITH HEADERS FROM 'file:///person_speaks_language.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: row.Person_id})
MERGE (l:Lenguage {language: row.language})
MERGE (p)-[:person_speaks_language]->(l);

// Relacion person-studyAt->organisation
LOAD CSV WITH HEADERS FROM 'file:///person_studyAt_organisation.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: row.Person_id})
MATCH (o:Organisation {id: row.Organisation_id})
MERGE (p)-[po:STUDY_AT {classYear: row.classYear}]->(o);

// Relacion person-workAt->organisation
LOAD CSV WITH HEADERS FROM 'file:///person_workAt_organisation.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: row.Person_id})
MATCH (o:Organisation {id: row.Organisation_id})
MERGE (p)-[po:WORK_AT {workFrom: row.workFrom}]->(o);

// Relacion post-hasCreator->person
LOAD CSV WITH HEADERS FROM 'file:///post_hasCreator_person.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Post {id: row.Post_id})
MATCH (pp:Person {id: row.Person_id})
CREATE (p)-[:HAS_CREATOR]->(pp);

// Relacion post-hasTag->tag
LOAD CSV WITH HEADERS FROM 'file:///post_hasTag_tag.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Post {id: row.Post_id})
MATCH (t:Tag {id: row.Tag_id})
CREATE (p)-[:HAS_TAG]->(t);

// Relacion post-isLocatedIn->place
LOAD CSV WITH HEADERS FROM 'file:///post_isLocatedIn_place.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Post {id: row.Post_id})
MATCH (pp:Place {id: row.Place_id})
CREATE (p)-[:IS_LOCATED_IN]->(pp);



// renombrar relaciones
MATCH (c:Comment)-[r:comment_hasCreator_person]->(p:Person)
CREATE (c)-[newRel:HAS_CREATOR]->(p)

MATCH (c:Comment)-[r:comment_hasTag_tag]->(t:Tag)
CREATE (c)-[newRel:HAS_TAG]->(t)

MATCH (c:Comment)-[r:comment_isLocatedIn_place]->(p:Place)
CREATE (c)-[newRel:IS_LOCATED_IN]->(p)

MATCH (p:Person)-[r:person_likes_comment]->(c:Comment)
CREATE (p)-[newRel:LIKES {creationDate: datetime(row.creationDate)}]->(c)

MATCH (p:Person)-[r:person_likes_comment]->(c:Comment)
CREATE (p)-[newRel:LIKES {creationDate: datetime(row.creationDate)}]->(c)


///////////////////////////////////

// Carga diseno fisico //

// Consulta 7
:auto MATCH (message1:Message)-[:HAS_CREATOR]->(person1:Person)
CALL {
    WITH message1, person1
    CREATE (message1)-[:BI7_message1_person1]->(person1)
} IN TRANSACTIONS OF 10000 ROWS;

:auto MATCH (person2:Person)<-[:HAS_CREATOR]-(message3:Message)<-[like:LIKES]-(p3:Person)
CALL {
    WITH person2, p3
    CREATE (p3)-[:BI7_person_person]->(person2)
} IN TRANSACTIONS OF 10000 ROWS;


// Consulta 8
:auto MATCH (tag:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)
CALL {
    WITH tag, comment
    CREATE (tag)<-[:BI8_comment_tag]-(comment)
} IN TRANSACTIONS OF 10000 ROWS;


// Consulta 9
:auto MATCH (post:Post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)
CALL {
    WITH post, tagClass
    CREATE (post)-[:BI9_post_tagClass]->(tagClass)
} IN TRANSACTIONS OF 10000 ROWS;

:auto MATCH (forum:Forum)-[:HAS_MEMBER]->(person:Person)
CALL {
    WITH forum, person
    CREATE (forum)-[:BI9_forum_person]->(person)
} IN TRANSACTIONS OF 10000 ROWS;


// Consulta 10
:auto MATCH (tag:Tag {name: 'John_Rhys-Davies'})<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
CALL {
    WITH message, person
    CREATE (message)-[:BI10_message_person]->(person)
} IN TRANSACTIONS OF 10000 ROWS;

//
:auto MATCH (tag:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
CALL {
    WITH message, person
    CREATE (message)-[:BI10_message_person]->(person)
} IN TRANSACTIONS OF 10000 ROWS;


// sol c
:auto MATCH (tag:Tag {name: 'John_Rhys-Davies'})<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person:Person)
WHERE datetime(message.creationDate) > datetime("2012-01-22T00:00:00.000+0000")
CALL {
    WITH tag, message, person
    CREATE (message)-[:BI10_message_person]->(person)
} IN TRANSACTIONS OF 10000 ROWS;


// Consulta 17
:auto MATCH (a:Person)-[:KNOWS]->(b:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country:Country)
CALL {
    WITH a, b
    CREATE (a)-[:BI17_person]->(b)
} IN TRANSACTIONS OF 1000 ROWS;


// Consulta 20
:auto MATCH (tagClass:TagClass)<-[:IS_SUBCLASS_OF*0..]-(:TagClass)<-[:HAS_TYPE]-(tag:Tag)<-[:HAS_TAG]-(message:Message)
CALL {
    WITH tagClass, message
    CREATE (message)-[:BI20_message_tagClass]->(tagClass)
} IN TRANSACTIONS OF 1000 ROWS;


// Consulta 21
:auto MATCH (country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)<-[:HAS_CREATOR]-(message:Message)
CALL {
    WITH country, zombie, message
    CREATE (message)-[:BI21_message_person]->(zombie)
} IN TRANSACTIONS OF 10000 ROWS;


// Consulta 22
:auto MATCH (person1:Person)
MATCH (person1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(:Message)-[:HAS_CREATOR]->(person2:Person)
CALL {
    WITH person1, person2
    CREATE (person1)-[:BI22_person_person_subscore1_2]->(person2)
} IN TRANSACTIONS OF 10000 ROWS;


:auto MATCH (person1:Person)
MATCH (person1)-[:LIKES]->(m:Message)-[:HAS_CREATOR]->(person2:Person)
CALL {
    WITH person1, person2
    CREATE (person1)<-[:BI22_person1_person2_subscore4_5]-(person2)
} IN TRANSACTIONS OF 10000 ROWS;


// Consulta 23
:auto MATCH (home:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_CREATOR]-(message:Message)
CALL {
    WITH home, message
    CREATE (home)<-[:BI23_message_country]-(message)
} IN TRANSACTIONS OF 10000 ROWS;


// Consulta 24
:auto MATCH (tagClass:TagClass)<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(message:Message)
CALL {
    WITH tagClass, message
    CREATE (tagClass)<-[:BI24_tagClass_message]-(message)
} IN TRANSACTIONS OF 10000 ROWS;

:auto MATCH (message:Message)-[:IS_LOCATED_IN]->(:Country)-[:IS_PART_OF]->(continent:Continent)
CALL {
    WITH message, continent
    CREATE (message)-[:BI24_message_continent]->(continent)
} IN TRANSACTIONS OF 10000 ROWS;

// Consulta 25
:auto MATCH (pA)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)
CALL {
    WITH pA, post
    CREATE (pA)-[c:BI25_reply_post]->(post)
} IN TRANSACTIONS OF 10000 ROWS;

:auto MATCH (pA)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)
CALL {
    WITH pA, c2
    CREATE (pA)-[c1:BI25_reply_comment]->(c2)
} IN TRANSACTIONS OF 10000 ROWS;



// Para Eliminar
:auto MATCH ()-[r:BI20_message_tagClass]->()
CALL {
    WITH r
    DELETE r
} IN TRANSACTIONS OF 10000 ROWS;
