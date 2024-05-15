using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15983 : RavenTestBase
    {
        public RavenDB_15983(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {

        }
        private class IndexJs : AbstractJavaScriptIndexCreationTask
        {
            public IndexJs()
            {
                Maps = new HashSet<string>
                {
                    @"
map('Items', function(item) {
    return {
        _: [
            createField('foo', 'a', { indexing: 'Default', storage: false, termVector: null }),
            createField('foo', 'b', { indexing: 'Default', storage: false, termVector: null })
        ]
    };
})"
                };
            }
        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items =>
                    from item in items
                    select new
                    {
                        _ = new[]
                        {
                            CreateField("foo", "a"),
                            CreateField("foo", "b"),
                        }
                    };
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanIndexMultipleFieldsWithSameNameUsingCreateFieldJavascript(Options options) => CanIndexMultipleFieldsWithSameNameUsingCreateFieldBase<IndexJs>(options);
        
        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanIndexMultipleFieldsWithSameNameUsingCreateField(Options options) => CanIndexMultipleFieldsWithSameNameUsingCreateFieldBase<Index>(options);
        
        private void CanIndexMultipleFieldsWithSameNameUsingCreateFieldBase<TIndex>(Options options) where TIndex : AbstractIndexCreationTask, new()
        {
            using var store = GetDocumentStore(options);
            var index = new TIndex(); 
            index.Execute(store);
            using (var session = store.OpenSession())
            {
                session.Store(new Item());
                session.SaveChanges();
            }
            Indexes.WaitForIndexing(store);
            WaitForUserToContinueTheTest(store);
            using (var session = store.OpenSession())
            {
                var c = session.Advanced.RawQuery<object>($"from index '{index.IndexName}' where foo = 'a'").Count();
                Assert.Equal(1, c);
                c = session.Advanced.RawQuery<object>($"from index '{index.IndexName}' where foo = 'b'").Count();
                Assert.Equal(1, c);
            }
        }
    }
}
