extern alias client;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Xunit;
using client::Raven.Bundles.Authorization.Model;
using client::Raven.Client.Authorization;

namespace Raven.Bundles.Tests.Authorization.Bugs.Wayne
{
    public class QueryTransformer : AuthorizationTest
    {
        [Fact]
        public void CanNotGetMessagesForRecipientWhenNotInRole()
        {
            List<WallMessage<AuthorizationUser>> messages;
            RavenQueryStatistics stats;

            var requester = new AuthorizationUser
            {
                Name = "requester",
                Id = "AuthorizationUsers-2"
            };

            var sender = new AuthorizationUser
            {
                Name = "sender",
                Id = "AuthorizationUsers-3"
            };

            var recipient = new AuthorizationUser
            {
                Name = "recipient",
                Id = "AuthorizationUsers-1"
            };

            var wallMessage = new WallMessage<AuthorizationUser>
            {
                Id = "WallMessageOfAuthorizationUsers-1",
                Creator = sender,
                Recipient = recipient,
                MessageBody = "blah"
            };

            new MessagesForAccountByCreatorId().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(recipient);

                session.Store(requester);

                session.Store(sender);

                session.Store(wallMessage);

                session.SaveChanges();

                //set view permissions only for recipient and sender

                session.SetAuthorizationFor(wallMessage, new DocumentAuthorization
                {
                    Permissions = {
                                new DocumentPermission
                                    {
                                        Role = "Authorization/Roles/FriendsOf/" + wallMessage.Recipient.Id,
                                        Allow = true,
                                        Operation = "WallMessage/View"
                                    },
                                new DocumentPermission
                                    {
                                        Allow = true,
                                        Operation = "WallMessage/View",
                                        User = wallMessage.Recipient.Id
                                    }
                            }
                });

                session.SaveChanges();
            }


            using (var session = store.OpenSession())
            {
                session.SecureFor(requester.Id, "WallMessage/View");

                messages = session.Query<WallMessage<AuthorizationUser>>().TransformWith<MessagesForAccountByCreatorId, WallMessage<AuthorizationUser>>()
                    .Statistics(out stats)
                    .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                    .Where(x => x.Recipient.Id == recipient.Id)
                    .ToList();
            }

            Assert.True(messages.Count == 0);
        }
    }
}
