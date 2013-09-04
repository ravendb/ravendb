// -----------------------------------------------------------------------
//  <copyright file="ListStorageActionsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Storage.Voron
{
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Database.Storage;
	using Raven.Json.Linq;

	using Xunit;

	using global::Voron.Exceptions;

	public class ListStorageActionsTests : RavenTest
	{
		[Fact]
		public void Set()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "voron"))
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

		[Fact]
		public void CannotAddTwoIdenticalKeys()
		{
			var e = Assert.Throws<ConcurrencyException>(
				() =>
				{
					using (var storage = NewTransactionalStorage(requestedStorage: "voron"))
					{
						storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "123" } }, UuidType.ReduceResults));
						storage.Batch(accessor => accessor.Lists.Set("name1", "key1", new RavenJObject { { "data", "321" } }, UuidType.ReduceResults));
					}
				});

			Assert.Equal("Cannot add 'name1/key1'. Version mismatch. Expected: 0. Actual: 1.", e.Message);
		}

		[Fact]
		public void CanAddTwoDifferentKeys()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "voron"))
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

		[Fact]
		public void Remove()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "voron"))
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

		[Fact]
		public void Read1()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "voron"))
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
					Assert.Equal(3, items.Count);
					CompareListItems(item1, items[0]);
					CompareListItems(item2, items[1]);
					CompareListItems(item3, items[2]);

					items = accessor.Lists.Read("name1", item1.Etag, null, 2).ToList();
					Assert.Equal(2, items.Count);
					CompareListItems(item1, items[0]);
					CompareListItems(item2, items[1]);

					items = accessor.Lists.Read("name1", item1.Etag, item2.Etag, 10).ToList();
					Assert.Equal(2, items.Count);
					CompareListItems(item1, items[0]);
					CompareListItems(item2, items[1]);

					items = accessor.Lists.Read("name1", item2.Etag, null, 10).ToList();
					Assert.Equal(2, items.Count);
					CompareListItems(item2, items[0]);
					CompareListItems(item3, items[1]);
				});
			}
		}

		[Fact]
		public void Read2()
		{
			using (var storage = NewTransactionalStorage(requestedStorage: "voron"))
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

		private void CompareListItems(ListItem expected, ListItem actual)
		{
			Assert.Equal(expected.Key, actual.Key);
			Assert.Equal(expected.Etag, actual.Etag);
			Assert.Equal(expected.Data["data"], actual.Data["data"]);
		}
	}
}