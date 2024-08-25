from neo4j import GraphDatabase

"""
This code optimizes the social network API by incorporating tags to categorize users by interests and attributes. 
It uses Neo4j to enhance search and organization, streamlining user, connection, and message management.
"""

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
    
    def deleteAll(self):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteAll)
            except Exception as e:
                print(f"Error deleting all content from the database: {e}")
    
    def deleteUser(self, userId):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteUser, userId)
            except Exception as e:
                print(f"Error deleting the user: {e}")

    def deleteCompany(self, userId):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteCompany, userId)
            except Exception as e:
                print(f"Error deleting the company: {e}")

    def deleteUniversity(self, userId):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteUniversity, userId)
            except Exception as e:
                print(f"Error deleting the university: {e}")

    def deleteConnection(self, userId1, userId2):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteConnection, userId1, userId2)
            except Exception as e:
                print(f"Error deleting the connection: {e}")

    def deleteMessage(self, messageId):
        with self.driver.session() as session:
            try:
                session.execute_write(self._deleteMessage, messageId)
            except Exception as e:
                print(f"Error deleting the message and its connections: {e}")

    @staticmethod
    def _deleteAll(tx):
        query = "MATCH (n) DETACH DELETE n"
        tx.run(query)

    @staticmethod
    def _deleteUser(tx, userId):
        query = "MATCH (u:User {userId: $userId}) DETACH DELETE u"
        tx.run(query, userId=userId)

    @staticmethod
    def _deleteCompany(tx, userId):
        query = "MATCH (u:User:Company {userId: $userId}) DETACH DELETE u"
        tx.run(query, userId=userId)

    @staticmethod
    def _deleteUniversity(tx, userId):
        query = "MATCH (u:User:University {userId: $userId}) DETACH DELETE u"
        tx.run(query, userId=userId)

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
            """
            MATCH (message:Message)-[:SENT]->(sender:User)-[outgoing:CONNECTED_TO]->(receiver:User)<-[:RECEIVED]-(message)
            WHERE id(message) = $messageId
            DELETE outgoing, message
            """
        )
        tx.run(query, messageId=messageId)
    
    def createUser(self, userId, name, tags=None):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createUser, userId, name, tags)
            except Exception as e:
                print(f"Error creating the user: {e}")

    def createCompany(self, userId, name):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createCompany, userId, name)
            except Exception as e:
                print(f"Error creating the company: {e}")

    def createUniversity(self, userId, name):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createUniversity, userId, name)
            except Exception as e:
                print(f"Error creating the university: {e}")

    def createConnection(self, userId1, userId2, connectionType):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createConnection, userId1, userId2, connectionType)
            except Exception as e:
                print(f"Error creating the connection: {e}")

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
    def _createUser(tx, userId, name, tags):
        query = "MERGE (u:User {userId: $userId, name: $name})"
        if tags:
            for tag in tags:
                query += f" SET u:{tag}"
        tx.run(query, userId=userId, name=name)

    @staticmethod
    def _createCompany(tx, userId, name):
        query = "MERGE (u:User:Company {userId: $userId, name: $name})"
        tx.run(query, userId=userId, name=name)

    @staticmethod
    def _createUniversity(tx, userId, name):
        query = "MERGE (u:User:University {userId: $userId, name: $name})"
        tx.run(query, userId=userId, name=name)
    
    @staticmethod
    def _createConnection(tx, userId1, userId2, connectionType):
        query = (
            """
            MATCH (u1:User {userId: $userId1})
            MATCH (u2:User {userId: $userId2})
            MERGE (u1)-[:CONNECTED_TO {type: $connectionType}]->(u2)
            """
        )
        tx.run(query, userId1=userId1, userId2=userId2, connectionType=connectionType)

    @staticmethod
    def _getFriendsAndFamily(tx, userId):
        query = (
            """
            MATCH (user:User {userId: $userId})-[:CONNECTED_TO]-(connection)
            RETURN connection
            """
        )
        result = tx.run(query, userId=userId)
        return [record["connection"] for record in result]

    @staticmethod
    def _getFamilyOfFamily(tx, userId):
        query = (
            """
            MATCH (user:User {userId: $userId})-[:CONNECTED_TO {type: 'family'}]-(family)-[:CONNECTED_TO {type: 'family'}]-(familyOfFamily)
            RETURN familyOfFamily
            """
        )
        result = tx.run(query, userId=userId)
        return [record["familyOfFamily"] for record in result]
    
    def createMessage(self, senderId, receiverId, content, timestamp):
        with self.driver.session() as session:
            try:
                session.execute_write(self._createMessage, senderId, receiverId, content, timestamp)
            except Exception as e:
                print(f"Error creating the message: {e}")

    def getMessagesAfterDate(self, senderId, receiverId, startDate):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._getMessagesAfterDate, senderId, receiverId, startDate)
            except Exception as e:
                print(f"Error retrieving messages after the date: {e}")

    def getFullConversation(self, userId1, userId2):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._getFullConversation, userId1, userId2)
            except Exception as e:
                print(f"Error retrieving the full conversation: {e}")

    @staticmethod
    def _createMessage(tx, senderId, receiverId, content, timestamp):
        query = (
            """
            MATCH (sender:User {userId: $senderId})
            MATCH (receiver:User {userId: $receiverId})
            CREATE (sender)-[:SENT]->(message:Message {content: $content, timestamp: $timestamp})-[:RECEIVED]->(receiver)
            """
        )
        tx.run(query, senderId=senderId, receiverId=receiverId, content=content, timestamp=timestamp)

    @staticmethod
    def _getMessagesAfterDate(tx, senderId, receiverId, startDate):
        query = (
            """
            MATCH (sender:User {userId: $senderId})-[:SENT]->(message:Message)-[:RECEIVED]->(receiver:User {userId: $receiverId})
            WHERE message.timestamp > $startDate
            RETURN message
            ORDER BY message.timestamp
            """
        )
        result = tx.run(query, senderId=senderId, receiverId=receiverId, startDate=startDate)
        return [record["message"] for record in result]

    @staticmethod
    def _getFullConversation(tx, userId1, userId2):
        query = (
            """
            MATCH (user1:User {userId: $userId1})-[:SENT]->(message:Message)-[:RECEIVED]->(user2:User {userId: $userId2})
            RETURN message
            ORDER BY message.timestamp
            """
        )
        result = tx.run(query, userId1=userId1, userId2=userId2)
        return [record["message"] for record in result]

    def getUsersByTag(self, tag):
        with self.driver.session() as session:
            try:
                return session.execute_read(self._getUsersByTag, tag)
            except Exception as e:
                print(f"Error retrieving users by tag: {e}")

    def addTagsToUser(self, userId, tags):
        with self.driver.session() as session:
            try:
                session.execute_write(self._addTagsToUser, userId, tags)
            except Exception as e:
                print(f"Error adding tags to user: {e}")

    def removeTagsFromUser(self, userId, tags):
        with self.driver.session() as session:
            try:
                session.execute_write(self._removeTagsFromUser, userId, tags)
            except Exception as e:
                print(f"Error removing tags from user: {e}")

    @staticmethod
    def _getUsersByTag(tx, tag):
        query = "MATCH (u:User) WHERE u:%s RETURN u" % tag
        result = tx.run(query)
        return [record["u"] for record in result]

    @staticmethod
    def _addTagsToUser(tx, userId, tags):
        query = "MATCH (u:User {userId: $userId})"
        for tag in tags:
            query += f" SET u:{tag}"
        tx.run(query, userId=userId)

    @staticmethod
    def _removeTagsFromUser(tx, userId, tags):
        query = "MATCH (u:User {userId: $userId})"
        for tag in tags:
            query += f" REMOVE u:{tag}"
        tx.run(query, userId=userId)
