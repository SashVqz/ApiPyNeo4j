from socialNetworkLegacy import SocialNetworkAPI

if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "Myrinealv1"

    api = SocialNetworkAPI(uri, username, password)

    try:
        api.delete()

        users = {
            "user1": "John Smith",
            "user2": "Emily Davis",
            "user3": "Michael Johnson",
            "user4": "Sarah Brown",
            "user5": "Jessica Taylor",
            "user6": "David Wilson",
            "user7": "Robert Miller",
            "user8": "Laura Martinez",
            "user9": "Olivia Anderson",
            "user10": "James White"
        }
        api.createUsers(users)

        universities = {
            "uni1": "Harvard University",
            "uni2": "Stanford University",
            "uni3": "University of Oxford",
            "uni4": "University of Cambridge",
            "uni5": "MIT",
            "uni6": "University of California, Berkeley"
        }
        api.createUniversities(universities)

        companies = {
            "comp1": "Google",
            "comp2": "Apple",
            "comp3": "Microsoft",
            "comp4": "Amazon",
            "comp5": "Facebook"
        }
        api.createCompanies(companies)

        connections = [
            ("user1", "user2", "friend"),
            ("user2", "user3", "family"),
            ("user3", "user4", "friend"),
            ("user4", "user5", "family"),
            ("user5", "user6", "friend"),
            ("user6", "user7", "family"),
            ("user7", "user8", "friend"),
            ("user8", "user9", "family"),
            ("user9", "user10", "friend"),
            ("user1", "comp1", "work"),
            ("user2", "comp2", "work"),
            ("user3", "comp3", "work"),
            ("user4", "comp4", "work"),
            ("user5", "comp5", "work"),
            ("user1", "uni1", "academic"),
            ("user2", "uni2", "academic"),
            ("user3", "uni3", "academic"),
            ("user4", "uni4", "academic"),
            ("user5", "uni5", "academic"),
            ("user6", "uni6", "academic")
        ]
        api.createConnections(connections)

        messages = [
            ('user1', 'user2', 'Hey, have you seen the latest news?', '2023-01-01T10:00:00'),
            ('user2', 'user1', 'No, what happened?', '2023-01-01T10:05:00'),
            ('user2', 'user3', 'We should catch up soon.', '2023-01-15T12:30:00'),
            ('user3', 'user2', 'Absolutely, let’s plan something.', '2023-01-15T12:45:00'),
            ('user4', 'user5', 'How’s your project going?', '2023-02-10T14:00:00'),
            ('user5', 'user4', 'Making good progress, thanks for asking!', '2023-02-10T14:10:00')
        ]
        api.createMessages(messages)

        # Fetch and print friends and family of user1
        friendsAndFamilyUser1 = api.getFriendsAndFamily("user1")
        print("Friends and family of user1:")
        print("\n".join(friendsAndFamilyUser1))

        print('\n')

        # Fetch and print family of friends of user1
        familyOfFriendsUser1 = api.getFamilyOfFamily("user1")
        print("Family of friends of user1:")
        print("\n".join(familyOfFriendsUser1))

        print('\n')

        # Fetch and print messages after a specific date
        messagesAfterDateUser1User2 = api.getMessagesAfterDate(senderId='user1', receiverId='user2', startDate='2023-01-01T00:00:00')
        print("Messages after the specified date between user1 and user2:")
        print("\n".join(messagesAfterDateUser1User2))

        print('\n')

        # Fetch and print full conversation between two users
        fullConversationUser1User2 = api.getFullConversation(userId1='user1', userId2='user2')
        if fullConversationUser1User2:
            print("Full conversation between user1 and user2:")
            print("\n".join(fullConversationUser1User2))
        else:
            print("Conversation not found between user1 and user2.")

        print('\n')

        # Create a post
        api.createPost(userId='user1', title='Hello World', content='Excited to connect with everyone! @user2 @user3', timestamp='2023-01-02T15:30:00')

        # Fetch and print users mentioned with work relation
        mentionedUsersUser1 = api.getUsersMentionedWithWorkRelation(userId='user1')
        print("Users mentioned with work relation by user1:")
        print("\n".join(mentionedUsersUser1))

        print('\n')

        # Fetch and print new connections by hops
        connectionsByHopsUser1 = api.findConnectionsByHops(userId='user1', maxHops=3)
        print("New connections by hops from user1:")
        for user, hops in connectionsByHopsUser1:
            print(f"User: {user}, Number of Hops: {hops}")

        print('\n')

        # Fetch and print new connections by messages
        connectionsByMessagesUser1 = api.findConnectionsByMessages(userId='user1', minMessages=2)
        print("New connections by messages from user1:")
        for user, messageCount in connectionsByMessagesUser1:
            print(f"User: {user}, Number of Messages: {messageCount}")

    finally:
        api.close()
