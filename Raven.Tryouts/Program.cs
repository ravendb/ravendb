using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Tests.Bugs;
using System.Linq;
using Raven.Tests.Faceted;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				Console.WriteLine();

				using(var x = new CompiledIndexesNhsevidence())
				{
					x.CanGetCorrectResults();
				}
			}
		}

		private static void MyTest()
		{
			var tx = new Raven.Storage.Managed.TransactionalStorage(new RavenConfiguration
			{
				RunInMemory = true
			}, () => { });
			tx.Initialize(new DummyUuidGenerator(), new OrderedPartCollection<AbstractDocumentCodec>());

			for (int xi = 0; xi < 5; xi++)
			{
				var wait = xi;
				Task.Factory.StartNew(() =>
				{
					Thread.Sleep(15*wait);
					tx.Batch(accessor =>
					{
						var reduceKeysAndBuckets = new List<ReduceKeyAndBucket>();
						for (int i = 0; i < 10; i++)
						{
							var docId = "users/" + i;
							reduceKeysAndBuckets.Add(new ReduceKeyAndBucket(IndexingUtil.MapBucket(docId), "1"));
							reduceKeysAndBuckets.Add(new ReduceKeyAndBucket(IndexingUtil.MapBucket(docId), "2"));
							accessor.MapReduce.PutMappedResult("test", docId, "1", new RavenJObject());
							accessor.MapReduce.PutMappedResult("test", docId, "2", new RavenJObject());
						}
						accessor.MapReduce.ScheduleReductions("test", 0, reduceKeysAndBuckets);
					});
				});

				Task.Factory.StartNew(() =>
				{
					Thread.Sleep(15 * wait);
					tx.Batch(accessor =>
					{
						var reduceKeysAndBuckets = new List<ReduceKeyAndBucket>();
						for (int i = 0; i < 10; i++)
						{
							var docId = "users/" + i;
							reduceKeysAndBuckets.Add(new ReduceKeyAndBucket(IndexingUtil.MapBucket(docId), "1"));
							reduceKeysAndBuckets.Add(new ReduceKeyAndBucket(IndexingUtil.MapBucket(docId), "2"));
							accessor.MapReduce.PutMappedResult("test3", docId, "1", new RavenJObject());
							accessor.MapReduce.PutMappedResult("test3", docId, "2", new RavenJObject());
						}
						accessor.MapReduce.ScheduleReductions("test3", 0, reduceKeysAndBuckets);
					});
				});
			}

			var items = 0;
			while (items != 100)
			{
				var itemsToDelete = new List<object>();
				tx.Batch(accessor =>
				{
					var list = accessor.MapReduce.GetItemsToReduce(
						index: "test",
						level: 0,
						take: 256,
						itemsToDelete: itemsToDelete
						).ToList();

					items += list.Count;
					Console.WriteLine(list.Count);
				});
				tx.Batch(accessor =>
				{
					accessor.MapReduce.DeleteScheduledReduction(itemsToDelete);
				});
				Thread.Sleep(10);
			}
		}

		private static void UseMyData()
		{
			using (var d = new MyData(new MemoryPersistentSource()))
			{
				using (d.BeginTransaction())
				{
					d.Documents.Put(new RavenJObject
						{
							{"key", "items/1"},
							{"id", "items/1"},
							{"etag", Guid.NewGuid().ToByteArray()},
						}, new byte[0]);
					d.Commit();
				}

				using (d.BeginTransaction())
				{
					d.Documents.Put(new RavenJObject
						{
							{"key", "items/1"},
							{"id", "items/1"},
							{"etag", Guid.NewGuid().ToByteArray()},
							{"txId", "1234"}
						}, new byte[0]);
					d.Transactions.Put(new RavenJObject
						{
							{"id", "1234"},
						}, new byte[0]);

					d.Commit();
				}

				ThreadPool.QueueUserWorkItem(state =>
					{
						d.BeginTransaction();
						Table.ReadResult readResult = d.Documents.Read(new RavenJObject {{"key", "items/1"}});
						var txId = readResult.Key.Value<string>("txId");

						Table.ReadResult txResult = d.Transactions.Read(new RavenJObject {{"id", txId}});

						if (txResult == null)
						{
							Environment.Exit(1);
							return;
						}

						d.Transactions.Remove(txResult.Key);

						var x = ((RavenJObject) readResult.Key.CloneToken());
						x.Remove("txId");

						d.Documents.UpdateKey(x);
						d.CommitCurrentTransaction();
					});


				while (true)
				{
					using (d.BeginTransaction())
					{
						Table.ReadResult readResult = d.Documents.Read(new RavenJObject {{"key", "items/1"}});
						var txId = readResult.Key.Value<string>("txId");

						if (txId == null)
						{
							return;
						}

						Table.ReadResult txResult = d.Transactions.Read(new RavenJObject {{"id", txId}});
						if (txResult == null)
						{
							Environment.Exit(1);
							return;
						}

						d.Commit();
					}
				}
			}
		}
	}

	public class MyData : Munin.Database
	{
		public MyData(IPersistentSource persistentSource)
			: base(persistentSource)
		{
			Documents = Add(new Table(x => x.Value<string>("key"), "Documents")
				{
					{"ByKey", x => x.Value<string>("key")},
					{"ById", x => x.Value<string>("id")},
					{"ByEtag", x => new ComparableByteArray(x.Value<byte[]>("etag"))}
				});

			Transactions = Add(new Table(x => x.Value<string>("txId"), "Transactions"));
		}

		public Table Transactions { get; set; }

		public Table Documents { get; set; }
	}
}