// -----------------------------------------------------------------------
//  <copyright file="Esent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Storage.Lists
{
	public class Munin : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public Munin()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				RunInMemory =  true
			});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}


		[Fact]
		public void CanAddAndReadByKey()
		{
			db.TransactionalStorage.Batch(actions => actions.Lists.Set("items", "1", new RavenJObject
			{
				{"test", "data"}
			}));

			db.TransactionalStorage.Batch(actions =>
			{
				var ravenJObject = actions.Lists.Read("items", "1");
				Assert.Equal("data", ravenJObject.Data.Value<string>("test"));
			});
		}


		[Fact]
		public void CanAddAndRemove()
		{
			db.TransactionalStorage.Batch(actions => actions.Lists.Set("items", "1", new RavenJObject
			{
				{"test", "data"}
			}));

			db.TransactionalStorage.Batch(actions =>
			{
				var ravenJObject = actions.Lists.Read("items", "1");
				Assert.Equal("data", ravenJObject.Data.Value<string>("test"));
			});

			db.TransactionalStorage.Batch(actions => actions.Lists.Remove("items", "1"));

			db.TransactionalStorage.Batch(actions => Assert.Null(actions.Lists.Read("items", "1")));
		}


		[Fact]
		public void CanReadByName()
		{
			for (int i = 0; i < 10; i++)
			{
				db.TransactionalStorage.Batch(
					actions => actions.Lists.Set("items", i.ToString(CultureInfo.InvariantCulture), new RavenJObject
					{
						{"i", i}
					}));
			}


			db.TransactionalStorage.Batch(actions =>
			{
				var list = actions.Lists.Read("items", Guid.Empty, 100).ToList();
				Assert.Equal(10, list.Count);
				for (int i = 0; i < 10; i++)
				{
					Assert.Equal(i, list[i].Data.Value<int>("i"));
				}
			});
		}

		[Fact]
		public void CanReadFromMiddle()
		{
			for (int i = 0; i < 10; i++)
			{
				db.TransactionalStorage.Batch(
					actions => actions.Lists.Set("items", i.ToString(CultureInfo.InvariantCulture), new RavenJObject
					{
						{"i", i}
					}));
			}


			db.TransactionalStorage.Batch(actions =>
			{
				var list = actions.Lists.Read("items", Guid.Empty, 5).ToList();
				Assert.Equal(5, list.Count);
				for (int i = 0; i < 5; i++)
				{
					Assert.Equal(i, list[i].Data.Value<int>("i"));
				}

				list = actions.Lists.Read("items", list.Last().Etag, 5).ToList();
				Assert.Equal(5, list.Count);
				for (int i = 0; i < 5; i++)
				{
					Assert.Equal(i+5, list[i].Data.Value<int>("i"));
				}
			});
		}

		[Fact]
		public void WillOnlyReadItemsFromTheSameList()
		{
			for (int i = 0; i < 10; i++)
			{
				db.TransactionalStorage.Batch(
					actions => actions.Lists.Set("items", i.ToString(CultureInfo.InvariantCulture), new RavenJObject
					{
						{"i", i}
					}));

				db.TransactionalStorage.Batch(
					actions => actions.Lists.Set("another", i.ToString(CultureInfo.InvariantCulture), new RavenJObject
					{
						{"i", i*2}
					}));
			}

			db.TransactionalStorage.Batch(actions =>
			{
				var list = actions.Lists.Read("items", Guid.Empty, 100).ToList();
				Assert.Equal(10, list.Count);
				for (int i = 0; i < 10; i++)
				{
					Assert.Equal(i, list[i].Data.Value<int>("i"));
				}
			});
		}
	}

}