using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15568 : RavenTestBase
    {
        public RavenDB_15568(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Skip = "Corax")]
        public void SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrorsInCorax(Options options) => SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors(
            options,
            simpleMapErrors =>
            {
                Assert.Equal(1, simpleMapErrors.Errors.Length);
                Assert.True(simpleMapErrors.Errors.All(x => x.Error.Contains("that is neither indexed nor stored is useless because it cannot be searched or retrieved.")));
            });

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrorsInLucene(Options options) => SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors(
            options,
            simpleMapErrors =>
            {
                Assert.Equal(25, simpleMapErrors.Errors.Length);
                Assert.True(simpleMapErrors.Errors.All(x => x.Error.Contains("it doesn't make sense to have a field that is neither indexed nor stored")));
            });
        
        
        private void SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors(Options options, Action<IndexErrors> assertion)
        {
            using (var store = GetDocumentStore(options))
            {
                new SimpleMapIndexWithDefaultFields().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 25; i++)
                        session.Store(new Company { Name = $"C_{i}", ExternalId = $"E_{i}" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = Indexes.WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);

                var simpleMapErrors = errors.Single(x => x.Name == new SimpleMapIndexWithDefaultFields().IndexName);
                assertion(simpleMapErrors);
            }
        }
        
        //A field `Name` that is neither indexed nor stored is useless because it cannot be searched or retrieved.

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
