using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class PlusInId : RavenTestBase
    {
        public class Document
        {
            public string Id { get; set; }
        }

        [Fact]
        public void WillSupportLast()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var docIdWithPlusSign = "Id+Character";
                var docIdWithoutPlusSign = "NormalId";

                using (var session = store.OpenSession())
                {
                    session.Store(new Document { Id = docIdWithPlusSign });
                    session.Store(new Document { Id = docIdWithoutPlusSign });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc1 = session
                        .Load<Document>(docIdWithPlusSign);
                    var doc2 = session
                        .Load<Document>(docIdWithoutPlusSign);

                    // pass
                    Assert.NotNull(doc1);
                    // pass
                    Assert.NotNull(doc2);
                }

                using (var session = store.OpenSession())
                {
                    var doc = session
                        .Load<Document>(docIdWithPlusSign, docIdWithoutPlusSign);

                    // pass
                    Assert.NotNull(doc[1]);

                    // fail
                    Assert.NotNull(doc[0]);
                }
            }
        }
    }
}