using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class WhereStringEqualsInCollection : RavenTestBase
    {
        public WhereStringEqualsInCollection(ITestOutputHelper output) : base(output)
        {
        }

        private void CreateData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new MyEntity { StringData = "Entity with collection", StringCollection = new List<string> { "CollectionItem1", "CollectionItem2" } });
                session.Store(new MyEntity { StringData = "entity with collection", StringCollection = new List<string> { "collectionitem1", "collectionitem2" } });
                session.SaveChanges();
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void AnyInCollectionEqualsConstant_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => o.StringCollection.Any(s => s.Equals("CollectionItem1")));

                    Assert.Equal(2, count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void AnyInCollectionEqualsConstant_IgnoreCase_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => o.StringCollection.Any(s => s.Equals("CollectionItem1", StringComparison.OrdinalIgnoreCase)));

                    Assert.Equal(2, count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void AnyInCollectionEqualsConstant_CaseSensitive_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Where(o => o.StringCollection.Any(s => s.Equals("CollectionItem1", StringComparison.Ordinal)));

                    Assert.Equal("from 'MyEntities' where exact(StringCollection = $p0)", query.ToString());
                    var result = query.ToList();
                    Assert.Equal(1, result.Count);
                    Assert.Equal("Entity with collection", result.First().StringData);

                }
            }
        }

        private class MyEntity
        {
            public MyEntity()
            {
                StringCollection = new List<string>();
            }

            public string StringData { get; set; }
            public ICollection<string> StringCollection { get; set; }
        }
    }
}
