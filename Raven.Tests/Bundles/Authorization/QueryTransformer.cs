using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Bundles.Authorization.Model;
using Raven.Client;
using Raven.Client.Authorization;
using Xunit;
using Assert = NetTopologySuite.Utilities.Assert;

namespace Raven.Tests.Bundles.Authorization
{
    public class QueryTransformer : RemoteClientTest
    {
        private readonly IDocumentStore store;

        public QueryTransformer()
        {
            store = NewRemoteDocumentStore();
        }

        public override void Dispose()
        {
            store.Dispose();
            base.Dispose();
        }

        [Fact]
        public void CanNotGetMessagesForRecipientWhenNotInRole()
        {
            List<WallMessage<AuthorizationUser>> messages;
            const int expectedResult = 0;
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

            Assert.IsTrue(messages.Count == 0);
        }
    }
}
