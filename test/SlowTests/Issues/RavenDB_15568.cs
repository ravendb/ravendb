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

        private Action<IndexErrors> _coraxAssertion = simpleMapErrors =>
        {
            Assert.Equal(25, simpleMapErrors.Errors.Length);
            Assert.True(simpleMapErrors.Errors.All(x => x.Error.Contains("that is neither indexed nor stored is useless because it cannot be searched or retrieved.")));
        };
        
        private Action<IndexErrors> _luceneAssertion = simpleMapErrors =>
        {
            Assert.Equal(25, simpleMapErrors.Errors.Length);
            Assert.True(simpleMapErrors.Errors.All(x => x.Error.Contains("it doesn't make sense to have a field that is neither indexed nor stored")));
        };
        
        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrorsInCorax(Options options) =>
            SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors<SimpleMapIndexWithDefaultFields>(options, _coraxAssertion);

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrorsInLucene(Options options) =>
            SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors<SimpleMapIndexWithDefaultFields>(options, _luceneAssertion);
        
        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void SettingDynamicFieldToNoIndexAndNoStoreShouldGenerateErrorsInCorax(Options options) =>
            SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors<DynamicItemWhichIsNotIndexed>(options, _coraxAssertion);

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void SettingDynamicFieldToNoIndexAndNoStoreShouldGenerateErrorsInLucene(Options options) =>
            SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors<DynamicItemWhichIsNotIndexed>(options, _luceneAssertion);
        
        private void SettingDefaultFieldsToNoIndexAndNoStoreShouldGenerateErrors<TIndex>(Options options, Action<IndexErrors> assertion) where TIndex : AbstractIndexCreationTask, new()
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
                WaitForUserToContinueTheTest(store);
                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = Indexes.WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);

                var simpleMapErrors = errors.Single(x => x.Name == new SimpleMapIndexWithDefaultFields().IndexName);
                assertion(simpleMapErrors);
            }
        }

        private class DynamicItemWhichIsNotIndexed : AbstractIndexCreationTask<Company>
        {
            public DynamicItemWhichIsNotIndexed()
            {
                Map = companies => companies.Select(doc => new
                {
                    Name = doc.Name, _ = CreateField("ExternalId", doc.ExternalId, new CreateFieldOptions() {Indexing = FieldIndexing.No, Storage = FieldStorage.No})
                });
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
