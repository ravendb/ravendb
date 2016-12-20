using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class Document
    {
        public string Id { get; set; }
        public ulong SomeNumber { get; set; }
    }

    public class RavenDb_5145: RavenTestBase
    {
        [Fact]
        public void TryToStoreBigInteger()
        {
            using (var store = NewDocumentStore())
            {
                string key;
                using (var session = store.OpenSession())
                {
                    var document = new Document
                    {
                        SomeNumber = 18446744073709551600
                    };
                    session.Store(document);
                    key = session.Advanced.GetDocumentId(document);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var document = session.Load<Document>(key);
                    Assert.NotNull(document);
                }
            }
        }
    }
}
