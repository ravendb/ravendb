using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Indexing
{
    public class RemoteIndexingOnDictionary : RavenTestBase
    {
        public RemoteIndexingOnDictionary(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanIndexOnRangeForNestedValuesForDictionaryAsPartOfDictionary(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new UserWithIDictionary
                    {
                        NestedItems = new Dictionary<string, NestedItem>
                            {
                                { "Color", new NestedItem{ Value=50 } }
                            }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Advanced.DocumentQuery<UserWithIDictionary>()
                     .WhereEquals("NestedItems.Key", "Color")
                     .AndAlso()
                     .WhereGreaterThan("NestedItems.Value.Value", 10)
                     .ToArray();
                }
            }

        }

        #region Nested type: UserWithIDictionary / NestedItem
        private class UserWithIDictionary
        {
            public string Id { get; set; }
            public IDictionary<string, string> Items { get; set; }
            public IDictionary<string, NestedItem> NestedItems { get; set; }
        }

        private class NestedItem
        {
            public string Name { get; set; }
            public double Value { get; set; }
        }

        #endregion
    }
}
