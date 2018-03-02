// -----------------------------------------------------------------------
//  <copyright file="RavenDB_10653.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Collections.Generic;
using Xunit;

namespace Raven.Tests.Core.Issues
{
    public class RavenDB_10653 : RavenCoreTestBase
    {
#if DNXCORE50
        public RavenDB_10653(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        [Fact]
        public void ConcurrentDictionaryShouldBeSerializedAndDeserializedCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var dictionary = new ConcurrentDictionary<string, Item.ItemType>();
                    dictionary.GetOrAdd("key1", s => new Item.ItemType { Name = "Name1" });
                    dictionary.GetOrAdd("key2", s => new Item.ItemType { Name = "Name2" });
                    dictionary.GetOrAdd("key3", s => null);

                    session.Store(new Item { Dictionary = dictionary }, "items/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");

                    Assert.True(item.Dictionary.GetType() == typeof(ConcurrentDictionary<string, Item.ItemType>));
                    Assert.Equal(3, item.Dictionary.Count);
                    Assert.Equal("Name1", item.Dictionary["key1"].Name);
                    Assert.Equal("Name2", item.Dictionary["key2"].Name);
                    Assert.Null(item.Dictionary["key3"]);
                }
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public IDictionary<string, ItemType> Dictionary { get; set; }

            public class ItemType
            {
                public string Name { get; set; }
            }
        }
    }
}