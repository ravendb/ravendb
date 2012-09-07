using System;
using System.Threading;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Munin;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			for (int i = 0; i < 10000; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				UseMyData();
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