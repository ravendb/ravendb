using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class WhereStringEqualsInCollection : RavenTest
    {
        private readonly EmbeddableDocumentStore store;

        public WhereStringEqualsInCollection()
        {
            store = NewDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new MyEntity { StringData = "Entity with collection", StringCollection = new List<string> { "CollectionItem1", "CollectionItem2" } });
                session.SaveChanges();
            }
        }

        [Fact]
        public void AnyInCollectionEqualsConstant_ShouldWork()
        {
            using (var session = store.OpenSession())
            {
                var count = session.Query<MyEntity>()
                                   .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                   .Count(o => o.StringCollection.Any(s => s.Equals("CollectionItem1")));

                Assert.Equal(1, count);
            }
        }

        [Fact]
        public void AnyInCollectionEqualsConstant_IgnoreCase_ShouldWork()
        {
            using (var session = store.OpenSession())
            {
                var count = session.Query<MyEntity>()
                                   .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                   .Count(o => o.StringCollection.Any(s => s.Equals("CollectionItem1", StringComparison.InvariantCultureIgnoreCase)));

                Assert.Equal(1, count);
            }
        }

        [Fact]
        public void AnyInCollectionEqualsConstant_CaseSensitive_ShouldWork()
        {
            using (var session = store.OpenSession())
            {
                Assert.Throws<NotSupportedException>(() => session.Query<MyEntity>()
                                                                  .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                                                  .Count(o => o.StringCollection.Any(s => s.Equals("CollectionItem1", StringComparison.Ordinal))));

            }
        }

        public class MyEntity
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