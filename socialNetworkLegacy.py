from neo4j import GraphDatabase

class SocialNetworkAPI:
    def __init__(self, uri, user, password):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def close(self):
        try:
            self.driver.close()
        except Exception as e:
            print(f"Error closing the database connection: {e}")

    def delete(self):
        with self.driver.session() as session:
            try:
                session.execute_write(self._delete)
            except Exception as e:
                print(f"Error deleting database content: {e}")

    def deleteUser(self, userId):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteUser, userId)
            except Exception as e:
                print(f"Error deleting user: {e}")

    def deleteCompany(self, companyId):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteCompany, companyId)
            except Exception as e:
                print(f"Error deleting company: {e}")

    def deleteUniversity(self, universityId):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteUniversity, universityId)
            except Exception as e:
                print(f"Error deleting university: {e}")

    def deleteConnection(self, userId1, userId2):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteConnection, userId1, userId2)
            except Exception as e:
                print(f"Error deleting connection: {e}")

    def deleteMessage(self, messageId):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteMessage, messageId)
            except Exception as e:
                print(f"Error deleting message and its connections: {e}")

    @staticmethod
    def _delete(tx):
        query = "MATCH (n) DETACH DELETE n"
        tx.run(query)

    @staticmethod
    def _deleteUser(tx, userId):
        query = "MATCH (u:User {userId: $userId}) DETACH DELETE u"
        tx.run(query, userId=userId)

    @staticmethod
    def _deleteCompany(tx, companyId):
        query = "MATCH (u:User:Company {userId: $companyId}) DETACH DELETE u"
        tx.run(query, companyId=companyId)

    @staticmethod
    def _deleteUniversity(tx, universityId):
        query = "MATCH (u:User:University {userId: $universityId}) DETACH DELETE u"
        tx.run(query, universityId=universityId)

    @staticmethod
    def _deleteConnection(tx, userId1, userId2):
        query = (
            "MATCH (u1:User {userId: $userId1})-[connection:CONNECTED_TO]-(u2:User {userId: $userId2}) "
            "DELETE connection"
        )
        tx.run(query, userId1=userId1, userId2=userId2)

    @staticmethod
    def _deleteMessage(tx, messageId):
        query = (
            "MATCH (message:Message)-[:SENT]->(sender:User)-[outgoing:CONNECTED_TO]->(receiver:User)<-[:RECEIVED]-(message) "
            "WHERE id(message) = $messageId "
            "DELETE outgoing, message"
        )
        tx.run(query, messageId=messageId)

    def createUser(self, userId, name):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createUser, userId, name)
            except Exception as e:
                print(f"Error creating user: {e}")

    def createCompany(self, companyId, name):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createCompany, companyId, name)
            except Exception as e:
                print(f"Error creating company: {e}")

    def createUniversity(self, universityId, name):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createUniversity, universityId, name)
            except Exception as e:
                print(f"Error creating university: {e}")

    def createConnection(self, userId1, userId2, connectionType):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createConnection, userId1, userId2, connectionType)
            except Exception as e:
                print(f"Error creating connection: {e}")

    def getFriendsAndFamily(self, userId):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._getFriendsAndFamily, userId)
            except Exception as e:
                print(f"Error retrieving friends and family: {e}")

    def getFamilyOfFamily(self, userId):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._getFamilyOfFamily, userId)
            except Exception as e:
                print(f"Error retrieving family of family: {e}")

    @staticmethod
    def _createUser(tx, userId, name):
        query = "MERGE (u:User {userId: $userId}) SET u.name = $name"
        tx.run(query, userId=userId, name=name)

    @staticmethod
    def _createCompany(tx, companyId, name):
        query = "MERGE (u:User:Company {userId: $companyId}) SET u.name = $name"
        tx.run(query, companyId=companyId, name=name)

    @staticmethod
    def _createUniversity(tx, universityId, name):
        query = "MERGE (u:User:University {userId: $universityId}) SET u.name = $name"
        tx.run(query, universityId=universityId, name=name)

    @staticmethod
    def _createConnection(tx, userId1, userId2, connectionType):
        query = (
            "MATCH (u1:User {userId: $userId1}), (u2:User {userId: $userId2}) "
            "MERGE (u1)-[:CONNECTED_TO {type: $connectionType}]->(u2)"
        )
        tx.run(query, userId1=userId1, userId2=userId2, connectionType=connectionType)

    @staticmethod
    def _getFriendsAndFamily(tx, userId):
        query = (
            "MATCH (user:User {userId: $userId})-[:CONNECTED_TO]-(connection) "
            "RETURN connection"
        )
        result = tx.run(query, userId=userId)
        return [record["connection"] for record in result]

    @staticmethod
    def _getFamilyOfFamily(tx, userId):
        query = (
            "MATCH (user:User {userId: $userId})-[:CONNECTED_TO {type: 'family'}]-(family)-[:CONNECTED_TO {type: 'family'}]-(familyOfFamily) "
            "RETURN familyOfFamily"
        )
        result = tx.run(query, userId=userId)
        return [record["familyOfFamily"] for record in result]

    def createMessage(self, senderId, receiverId, content, timestamp):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createMessage, senderId, receiverId, content, timestamp)
            except Exception as e:
                print(f"Error creating message: {e}")

    def getMessagesAfterDate(self, senderId, receiverId, startDate):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._getMessagesAfterDate, senderId, receiverId, startDate)
            except Exception as e:
                print(f"Error retrieving messages after date: {e}")

    def getFullConversation(self, userId1, userId2):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._getFullConversation, userId1, userId2)
            except Exception as e:
                print(f"Error retrieving full conversation: {e}")

    @staticmethod
    def _createMessage(tx, senderId, receiverId, content, timestamp):
        query = (
            "MATCH (sender:User {userId: $senderId}), (receiver:User {userId: $receiverId}) "
            "CREATE (sender)-[:SENT]->(message:Message {content: $content, timestamp: $timestamp})-[:RECEIVED]->(receiver)"
        )
        tx.run(query, senderId=senderId, receiverId=receiverId, content=content, timestamp=timestamp)

    @staticmethod
    def _getMessagesAfterDate(tx, senderId, receiverId, startDate):
        query = (
            "MATCH (sender:User {userId: $senderId})-[:SENT]->(message:Message)-[:RECEIVED]->(receiver:User {userId: $receiverId}) "
            "WHERE message.timestamp > $startDate "
            "RETURN message"
        )
        result = tx.run(query, senderId=senderId, receiverId=receiverId, startDate=startDate)
        return [record["message"] for record in result]

    @staticmethod
    def _getFullConversation(tx, userId1, userId2):
        query = (
            "MATCH (user1:User {userId: $userId1})-[:SENT]->(message:Message)-[:RECEIVED]->(user2:User {userId: $userId2}) "
            "RETURN message "
            "UNION "
            "MATCH (user1:User {userId: $userId2})-[:SENT]->(message:Message)-[:RECEIVED]->(user2:User {userId: $userId1}) "
            "RETURN message"
        )
        result = tx.run(query, userId1=userId1, userId2=userId2)
        return [record["message"] for record in result]

    def createPost(self, userId, title, content, timestamp):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createPost, userId, title, content, timestamp)
            except Exception as e:
                print(f"Error creating post: {e}")

    def getUsersMentionedWithWorkRelation(self, userId):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._getUsersMentionedWithWorkRelation, userId)
            except Exception as e:
                print(f"Error retrieving users mentioned with work relation: {e}")

    @staticmethod
    def _createPost(tx, userId, title, content, timestamp):
        query = (
            "MATCH (user:User {userId: $userId}) "
            "CREATE (user)-[:POSTED]->(post:Post {title: $title, content: $content, timestamp: $timestamp}) "
            "WITH user, post, split($content, ' ') AS words "
            "UNWIND words AS word "
            "WITH user, post, word "
            "WHERE word STARTS WITH '@' "
            "MATCH (mentioned:User {userId: substring(word, 1)}) "
            "MERGE (user)-[:MENTIONED]->(mentioned)"
        )
        tx.run(query, userId=userId, title=title, content=content, timestamp=timestamp)

    @staticmethod
    def _getUsersMentionedWithWorkRelation(tx, userId):
        query = (
            "MATCH (user:User {userId: $userId})-[:MENTIONED]->(mentioned:User) "
            "MATCH (post:Post)<-[:POSTED]-(user) "
            "WHERE toLower(post.content) CONTAINS '@' + mentioned.userId "
            "RETURN DISTINCT mentioned"
        )
        result = tx.run(query, userId=userId)
        return [record["mentioned"] for record in result]

    def findConnectionsByHops(self, userId, maxHops):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._findConnectionsByHops, userId, maxHops)
            except Exception as e:
                print(f"Error finding new connections by hops: {e}")

    def findConnectionsByMessages(self, userId, minMessages):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._findConnectionsByMessages, userId, minMessages)
            except Exception as e:
                print(f"Error finding new connections by messages: {e}")

    @staticmethod
    def _findConnectionsByHops(tx, userId, maxHops):
        query = (
            "MATCH path = (user1:User {userId: $userId})-[:CONNECTED_TO*1..%d]-(user2:User)-[:CONNECTED_TO]-(user3:User) "
            "WHERE NOT (user1)-[:CONNECTED_TO]-(user3) "
            "AND length(path) <= %d "
            "RETURN DISTINCT user3, length(path) AS hops "
            "ORDER BY hops"
            % (maxHops, maxHops)
        )
        result = tx.run(query, userId=userId)
        return [(record["user3"], record["hops"]) for record in result]

    @staticmethod
    def _findConnectionsByMessages(tx, userId, minMessages):
        query = (
            "MATCH (user1:User {userId: $userId})-[:CONNECTED_TO*1..]-(user2:User)-[:SENT]->(:Message)-[:RECEIVED]->(user3:User) "
            "WHERE NOT (user1)-[:CONNECTED_TO]-(user3) "
            "AND EXISTS { MATCH (user1)-[:CONNECTED_TO*1..]-(user2) } "
            "WITH user3, COUNT(DISTINCT user2) AS message_count "
            "WHERE message_count >= $minMessages "
            "RETURN DISTINCT user3, message_count "
            "ORDER BY message_count DESC"
        )
        result = tx.run(query, userId=userId, minMessages=minMessages)
        return [(record["user3"], record["message_count"]) for record in result]
