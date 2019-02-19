using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class BooleanCollection : RavenTestBase
    {
        private class Message
        {
            public class Recipient
            {
                public string To { get; set; }
                public bool HasRead { get; set; }
                public int HasReadInt { get; set; }
            }

            public Message()
            {
                this.Recipients = new List<Recipient>();
            }
            public string Id { get; set; }
            public string From { get; set; }

            public IList<Recipient> Recipients { get; set; }
        }

        private class MessageIndex : AbstractIndexCreationTask<Message>
        {
            public MessageIndex()
            {
                // audio properties
                Map = messages => from msg in messages
                                  from recipient in msg.Recipients
                                  select new
                                  {
                                      Id = msg.Id,
                                      From = msg.From,
                                      Recipients_To = recipient.To,
                                      Recipients_HasRead = recipient.HasRead,
                                      Recipients_HasReadInt = recipient.HasReadInt
                                      //HasRead = recipient.HasRead
                                  };
            }
        }

        private void Setup(IDocumentStore store)
        {
            new MessageIndex().Execute(store);

            // create user
            using (var session = store.OpenSession())
            {
                var msg = new Message()
                {
                    Id = "messages/1",
                    From = "Paul",
                    Recipients = new List<Message.Recipient>()
                                                   {
                                                       new Message.Recipient()
                                                           {
                                                               To = "Joe",
                                                               HasRead = true,
                                                               HasReadInt = 1
                                                           }
                                                   }
                };
                session.Store(msg);
                session.SaveChanges();
            }
        }


        [Fact]
        public void IndexShouldAllowToQueryOnBooleanSubcollectionProperty()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                Message msg;
                using (var session = store.OpenSession())
                {
                    msg = session.Query<Message, MessageIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Id == "messages/1")
                        .Where(x => x.Recipients.Any(a => a.HasRead && a.To == "Joe"))
                        .SingleOrDefault();
                }
                Assert.NotNull(msg);
            }
        }

        [Fact]
        public void IndexShouldAllowToQueryOnIntegerSubcollectionProperty()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                Message msg;
                using (var session = store.OpenSession())
                {
                    msg = session.Query<Message, MessageIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Id == "messages/1")
                        .Where(x => x.Recipients.Any(a => a.HasReadInt == 1 && a.To == "Joe"))
                        .SingleOrDefault();
                }
                Assert.NotNull(msg);
            }
        }

    }
}
