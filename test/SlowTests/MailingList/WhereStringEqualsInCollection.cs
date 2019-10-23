using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
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
                session.SaveChanges();
            }
        }

        [Fact]
        public void AnyInCollectionEqualsConstant_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => o.StringCollection.Any(s => s.Equals("CollectionItem1")));

                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public void AnyInCollectionEqualsConstant_IgnoreCase_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => o.StringCollection.Any(s => s.Equals("CollectionItem1", StringComparison.OrdinalIgnoreCase)));

                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public void AnyInCollectionEqualsConstant_CaseSensitive_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    Assert.Throws<NotSupportedException>(() => session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => o.StringCollection.Any(s => s.Equals("CollectionItem1", StringComparison.Ordinal))));

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
