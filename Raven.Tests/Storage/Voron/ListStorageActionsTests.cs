// -----------------------------------------------------------------------
//  <copyright file="ListStorageActionsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using Raven.Abstractions;
using Raven.Tests.Common;

namespace Raven.Tests.Storage.Voron
{
    using System.Linq;

    using Raven.Abstractions.Data;
    using Raven.Database.Storage;
    using Raven.Json.Linq;

    using Xunit;
    using Xunit.Extensions;

    using global::Voron.Exceptions;

    [Trait("VoronTest", "StorageActionsTests")]
    public class ListStorageActionsTests : TransactionalStorageTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public void Set(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));

                storage.Batch(accessor =>
                {
                    var item = accessor.Lists.Read("name1", "key1");

                    Assert.NotNull(item);
                    Assert.Equal("key1", item.Key);
                    Assert.Equal("123", item.Data.Value<string>("data"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CanAddTwoIdenticalKeys(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));
                storage.Batch(
                    accessor =>
                    {
                        var item = accessor.Lists.Read("name1", "key1");
                        Assert.NotNull(item);
                        Assert.Equal("key1", item.Key);
                        Assert.Equal("321", item.Data.Value<string>("data"));
                    });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CanAddTwoDifferentKeys(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key2", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));

                storage.Batch(accessor =>
                {
                    var item1 = accessor.Lists.Read("name1", "key1");
                    var item2 = accessor.Lists.Read("name1", "key2");

                    Assert.NotNull(item1);
                    Assert.Equal("key1", item1.Key);
                    Assert.Equal("123", item1.Data.Value<string>("data"));

                    Assert.NotNull(item2);
                    Assert.Equal("key2", item2.Key);
                    Assert.Equal("321", item2.Data.Value<string>("data"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void Remove(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key2", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));

                storage.Batch(accessor => accessor.Lists.Remove("name1", "key2"));

                storage.Batch(accessor =>
                {
                    var item1 = accessor.Lists.Read("name1", "key1");
                    var item2 = accessor.Lists.Read("name1", "key2");

                    Assert.NotNull(item1);
                    Assert.Equal("key1", item1.Key);
                    Assert.Equal("123", item1.Data.Value<string>("data"));

                    Assert.Null(item2);
                });

                storage.Batch(accessor => accessor.Lists.Remove("name1", "key1"));

                storage.Batch(accessor =>
                {
                    var item1 = accessor.Lists.Read("name1", "key1");
                    var item2 = accessor.Lists.Read("name1", "key2");

                    Assert.Null(item1);
                    Assert.Null(item2);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void Read1(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key2", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key3", new RavenJObject { { "data", "213" } }, UuidType.ReduceResults));

                storage.Batch(accessor =>
                {
                    var item1 = accessor.Lists.Read("name1", "key1");
                    var item2 = accessor.Lists.Read("name1", "key2");
                    var item3 = accessor.Lists.Read("name1", "key3");

                    var items = accessor.Lists.Read("name1", item1.Etag, null, 10).ToList();
                    Assert.Equal(2, items.Count);
                    CompareListItems(item2, items[0]);
                    CompareListItems(item3, items[1]);

                    items = accessor.Lists.Read("name1", item1.Etag, null, 2).ToList();
                    Assert.Equal(2, items.Count);
                    CompareListItems(item2, items[0]);
                    CompareListItems(item3, items[1]);

                    items = accessor.Lists.Read("name1", item1.Etag, item2.Etag, 10).ToList();
                    Assert.Equal(0, items.Count);

                    items = accessor.Lists.Read("name1", item2.Etag, null, 10).ToList();
                    Assert.Equal(1, items.Count);
                    CompareListItems(item3, items[0]);

                    items = accessor.Lists.Read("name1", item1.Etag, item3.Etag, 10).ToList();
                    Assert.Equal(1, items.Count);
                    CompareListItems(item2, items[0]);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void Read2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name2", "key1", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));

                storage.Batch(accessor =>
                {
                    var item1 = accessor.Lists.Read("name1", "key1");
                    var item2 = accessor.Lists.Read("name2", "key1");

                    Assert.NotNull(item1);
                    Assert.Equal("key1", item1.Key);
                    Assert.Equal("123", item1.Data.Value<string>("data"));

                    Assert.NotNull(item2);
                    Assert.Equal("key1", item2.Key);
                    Assert.Equal("321", item2.Data.Value<string>("data"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ReadLast(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key9", new RavenJObject { { "data", "111" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key5", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name2", "key2", new RavenJObject { { "data", "000" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name3", "key7", new RavenJObject { { "data", "000" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Remove("name3", "key7"));

                storage.Batch(accessor =>
                {
                    var item1 = accessor.Lists.ReadLast("name1");
                    var item2 = accessor.Lists.ReadLast("name2");
                    var item3 = accessor.Lists.ReadLast("name3");

                    Assert.NotNull(item1);
                    Assert.Equal("key5", item1.Key);
                    Assert.Equal("321", item1.Data.Value<string>("data"));

                    Assert.NotNull(item2);
                    Assert.Equal("key2", item2.Key);
                    Assert.Equal("000", item2.Data.Value<string>("data"));

                    Assert.Null(item3);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void RemoveBefore(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key2", new RavenJObject { { "data", "111" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name2", "key1", new RavenJObject { { "data", "000" } }, UuidType.ReduceResults));

                Etag name1Etag = null;
                Etag name2Etag = null;
                storage.Batch(accessor =>
                {
                    name1Etag = accessor.Lists.Read("name1", Etag.Empty, null, 10).Max(x => x.Etag);
                    name2Etag = accessor.Lists.Read("name2", Etag.Empty, null, 10).Max(x => x.Etag);
                });

                storage.Batch(accessor => accessor.Lists.Set("name1", "key3", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name2", "key2", new RavenJObject { { "data", "000" } }, UuidType.ReduceResults));

                storage.Batch(accessor =>
                {
                    accessor.Lists.RemoveAllBefore("name1", name1Etag);
                    accessor.Lists.RemoveAllBefore("name2", name2Etag);
                });

                storage.Batch(accessor =>
                {
                    var items1 = accessor.Lists.Read("name1", Etag.Empty, null, 10).ToList();
                    var items2 = accessor.Lists.Read("name2", Etag.Empty, null, 10).ToList();

                    Assert.Equal(1, items1.Count);
                    Assert.Equal("key3", items1[0].Key);
                    Assert.Equal("321", items1[0].Data.Value<string>("data"));

                    Assert.Equal(1, items2.Count);
                    Assert.Equal("key2", items2[0].Key);
                    Assert.Equal("000", items2[0].Data.Value<string>("data"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void RemoveOlderThan(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                SystemTime.UtcDateTime = () => DateTime.Now.AddDays(-2);

                storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name1", "key2", new RavenJObject { { "data", "111" } }, UuidType.ReduceResults));

                storage.Batch(accessor => accessor.Lists.Set("name2", "key1", new RavenJObject { { "data", "000" } }, UuidType.ReduceResults));

                SystemTime.UtcDateTime = () => DateTime.Now;

                storage.Batch(accessor => accessor.Lists.Set("name1", "key3", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));
                storage.Batch(accessor => accessor.Lists.Set("name2", "key2", new RavenJObject { { "data", "000" } }, UuidType.ReduceResults));

                storage.Batch(accessor =>
                {
                    accessor.Lists.RemoveAllOlderThan("name1", SystemTime.UtcNow.Subtract(TimeSpan.FromDays(1)));
                    accessor.Lists.RemoveAllOlderThan("name2", SystemTime.UtcNow.Subtract(TimeSpan.FromDays(1)));
                });

                storage.Batch(accessor =>
                {
                    var items1 = accessor.Lists.Read("name1", Etag.Empty, null, 10).ToList();
                    var items2 = accessor.Lists.Read("name2", Etag.Empty, null, 10).ToList();

                    Assert.Equal(1, items1.Count);
                    Assert.Equal("key3", items1[0].Key);
                    Assert.Equal("321", items1[0].Data.Value<string>("data"));

                    Assert.Equal(1, items2.Count);
                    Assert.Equal("key2", items2[0].Key);
                    Assert.Equal("000", items2[0].Data.Value<string>("data"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CanReadWithStartTakeParameters(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                for (int i = 0; i < 10; i++)
                {
                    storage.Batch(
                        actions => actions.Lists.Set("items", i.ToString(CultureInfo.InvariantCulture), new RavenJObject
                    {
                        {"i", i}
                    }, UuidType.Indexing));

                    storage.Batch(
                        actions => actions.Lists.Set("another", i.ToString(CultureInfo.InvariantCulture), new RavenJObject
                    {
                        {"i", i*2}
                    }, UuidType.Indexing));
                }

                storage.Batch(actions =>
                {
                    var list = actions.Lists.Read("items", 0, 5).ToList();
                    Assert.Equal(5, list.Count);
                    for (int i = 0; i < 5; i++)
                    {
                        Assert.Equal(i, list[i].Data.Value<int>("i"));
                    }

                    list = actions.Lists.Read("items", 5, 5).ToList();
                    Assert.Equal(5, list.Count);
                    for (int i = 0; i < 5; i++)
                    {
                        Assert.Equal(i + 5, list[i].Data.Value<int>("i"));
                    }
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ShouldUpdate(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions => actions.Lists.Set("items", "1", new RavenJObject
                {
                    {
                        "test", "data1"
                    }
                }, UuidType.Indexing));

                storage.Batch(actions => actions.Lists.Set("items", "1", new RavenJObject
                {
                    {
                        "test", "data2"
                    }
                }, UuidType.Indexing));

                storage.Batch(actions =>
                {
                    var item = actions.Lists.Read("items", "1");

                    Assert.Equal("data2", item.Data.Value<string>("test"));
                });

                storage.Batch(actions =>
                {
                    var items = actions.Lists.Read("items", 0, 10).ToList();

                    Assert.Equal(1, items.Count);
                    Assert.Equal("data2", items[0].Data.Value<string>("test"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void TouchingListItemIncreasesItsEtag(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions => actions.Lists.Set("items", "1", new RavenJObject
                {
                    {
                        "test", "data1"
                    }
                }, UuidType.Documents));

                Etag oldEtag = null;

                storage.Batch(actions =>
                {
                    oldEtag = actions.Lists.Read("items", "1").Etag;
                });

                storage.Batch(actions =>
                {
                    Etag afterEtag;
                    Etag beforeEtag;
                    actions.Lists.Touch("items", "1", UuidType.Documents, out beforeEtag, out afterEtag);

                    Assert.True(afterEtag.CompareTo(oldEtag) > 0);
                    Assert.True(oldEtag.CompareTo(beforeEtag) == 0);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void AfterTouchListItemDataRemainsTheSame(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions => actions.Lists.Set("items", "1", new RavenJObject
                {
                    {
                        "test", "data1"
                    }
                }, UuidType.Documents));

                DateTime? createdAt = null;
                storage.Batch(actions =>
                {
                    createdAt = actions.Lists.Read("items", "1").CreatedAt;
                });

                storage.Batch(actions =>
                {
                    Etag afterEtag;
                    Etag beforeEtag;
                    actions.Lists.Touch("items", "1", UuidType.Documents, out beforeEtag, out afterEtag);
                });

                storage.Batch(actions =>
                {
                    var item = actions.Lists.Read("items", "1");

                    Assert.Equal("data1", item.Data.Value<string>("test"));
                    Assert.Equal(createdAt, item.CreatedAt);
                });

                storage.Batch(actions =>
                {
                    var items = actions.Lists.Read("items", 0, 10).ToList();

                    Assert.Equal(1, items.Count);
                    Assert.Equal("data1", items[0].Data.Value<string>("test"));
                    Assert.Equal(createdAt, items[0].CreatedAt);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void TouchedItemShouldBeReturnedOnHigherPosition(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions => actions.Lists.Set("items", "1", new RavenJObject
                {
                    {
                        "test", "1"
                    }
                }, UuidType.Documents));

                storage.Batch(actions => actions.Lists.Set("items", "2", new RavenJObject
                {
                    {
                        "test", "2"
                    }
                }, UuidType.Documents));

                storage.Batch(actions =>
                {
                    Etag afterEtag;
                    Etag beforeEtag;
                    actions.Lists.Touch("items", "1", UuidType.Documents, out beforeEtag, out afterEtag);
                });

                storage.Batch(actions =>
                {
                    var items = actions.Lists.Read("items", 0, 100).ToList();

                    Assert.Equal("2", items[0].Key);
                    Assert.Equal("1", items[1].Key);

                    items = actions.Lists.Read("items", Etag.Empty, null, 10).ToList();

                    Assert.Equal("2", items[0].Key);
                    Assert.Equal("1", items[1].Key);
                });
            }
        }

        private void CompareListItems(ListItem expected, ListItem actual)
        {
            Assert.Equal(expected.Key, actual.Key);
            Assert.Equal(expected.Etag, actual.Etag);
            Assert.Equal(expected.Data["data"], actual.Data["data"]);
        }
    }
}
