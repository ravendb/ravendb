using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class NullableGuidIndexTest : RavenTestBase
    {
        private class TestDocument
        {
            public string Id { get; set; }

            public Guid? OptionalExternalId { get; set; }
        }

        private class TestDocumentIndex : AbstractIndexCreationTask<TestDocument>
        {
            public TestDocumentIndex()
            {
                Map = docs => from doc in docs
                              where doc.OptionalExternalId != null
                              select new { doc.OptionalExternalId };
            }
        }

        [Fact]
        public void Can_query_against_nullable_guid()
        {
            using (var store = GetDocumentStore())
            {
                new TestDocumentIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument());
                    session.Store(new TestDocument { OptionalExternalId = Guid.NewGuid() });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    TestDocument[] results = session.Query<TestDocument, TestDocumentIndex>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .ToArray();

                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.NotEmpty(results);
                    Assert.NotEmpty(results);
                }
            }
        }
    }
}
