using System.Linq;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15568 : RavenTestBase
    {
        public RavenDB_15568(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors()
        {
            using (var store = GetDocumentStore())
            {
                new SimpleMapIndexWithDefaultFields().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 25; i++)
                        session.Store(new Company { Name = $"C_{i}", ExternalId = $"E_{i}" });

                    session.SaveChanges();
                }

                WaitForIndexing(store, allowErrors: true);

                var errors = WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);

                var simpleMapErrors = errors.Single(x => x.Name == new SimpleMapIndexWithDefaultFields().IndexName);
                Assert.Equal(25, simpleMapErrors.Errors.Length);
                Assert.True(simpleMapErrors.Errors.All(x => x.Error.Contains("it doesn't make sense to have a field that is neither indexed nor stored")));
            }
        }

        private class SimpleMapIndexWithDefaultFields : AbstractIndexCreationTask<Company>
        {
            public SimpleMapIndexWithDefaultFields()
            {
                Map = companies => from c in companies
                    select new
                    {
                        c.Name,
                        c.ExternalId
                    };

                Index(Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);
                Store(Constants.Documents.Indexing.Fields.AllFields, FieldStorage.No);
            }
        }
    }
}
