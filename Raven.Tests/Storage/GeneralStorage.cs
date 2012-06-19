//-----------------------------------------------------------------------
// <copyright file="GeneralStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Tasks;
using Raven.Munin;
using Raven.Storage.Managed.Impl;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class GeneralStorage : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public GeneralStorage()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = DataDir,
			});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Can_query_by_id_prefix()
		{
			db.Put("abc", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
			db.Put("Raven/Databases/Hello", null, new RavenJObject {{"a", "b"}}, new RavenJObject(), null);
			db.Put("Raven/Databases/Northwind", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
			db.Put("Raven/Databases/Sys", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
			db.Put("Raven/Databases/Db", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);
			db.Put("Raven/Database", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);

			var dbs = db.GetDocumentsWithIdStartingWith("Raven/Databases/", 0, 10);

			Assert.Equal(4, dbs.Length);
		}

		[Fact]
		public void WhenPutAnIdWithASpace_IdWillBeAGuid()
		{
			db.Put(" ", null, new RavenJObject { { "a", "b" } }, new RavenJObject(), null);

			var doc = db.GetDocuments(0, 10, null)
				.OfType<RavenJObject>()
				.Single();
			var id = doc["@metadata"].Value<string>("@id");
			Assert.False(string.IsNullOrWhiteSpace(id));
			Assert.DoesNotThrow(() => new Guid(id)); 
		}

		[Fact]
		public void CanProperlyHandleDeletingThreeItemsBothFromPK_And_SecondaryIndexes()
		{
			var cmds = new[]
			{
				@"{""Cmd"":""Put"",""Key"":{""index"":""Raven/DocumentsByEntityName"",""id"":""AAAAAAAAAAEAAAAAAAAABQ=="",""time"":""\/Date(1290420997504)\/"",""type"":""Raven.Database.Tasks.RemoveFromIndexTask"",""mergable"":true},""TableId"":9,""TxId"":""NiAAMOT72EC/We7rnZS/Fw==""}"
				,
				@"{""Cmd"":""Put"",""Key"":{""index"":""Raven/DocumentsByEntityName"",""id"":""AAAAAAAAAAEAAAAAAAAABg=="",""time"":""\/Date(1290420997509)\/"",""type"":""Raven.Database.Tasks.RemoveFromIndexTask"",""mergable"":true},""TableId"":9,""TxId"":""NiAAMOT72EC/We7rnZS/Fw==""}"
				,
				@"{""Cmd"":""Put"",""Key"":{""index"":""Raven/DocumentsByEntityName"",""id"":""AAAAAAAAAAEAAAAAAAAABw=="",""time"":""\/Date(1290420997509)\/"",""type"":""Raven.Database.Tasks.RemoveFromIndexTask"",""mergable"":true},""TableId"":9,""TxId"":""NiAAMOT72EC/We7rnZS/Fw==""}"
				,
				@"{""Cmd"":""Commit"",""TableId"":9,""TxId"":""NiAAMOT72EC/We7rnZS/Fw==""}",
				@"{""Cmd"":""Del"",""Key"":{""index"":""Raven/DocumentsByEntityName"",""id"":""AAAAAAAAAAEAAAAAAAAABg=="",""time"":""\/Date(1290420997509)\/"",""type"":""Raven.Database.Tasks.RemoveFromIndexTask"",""mergable"":true},""TableId"":9,""TxId"":""wM3q3VA0XkWecl5WBr9Cfw==""}"
				,
				@"{""Cmd"":""Del"",""Key"":{""index"":""Raven/DocumentsByEntityName"",""id"":""AAAAAAAAAAEAAAAAAAAABw=="",""time"":""\/Date(1290420997509)\/"",""type"":""Raven.Database.Tasks.RemoveFromIndexTask"",""mergable"":true},""TableId"":9,""TxId"":""wM3q3VA0XkWecl5WBr9Cfw==""}"
				,
				@"{""Cmd"":""Del"",""Key"":{""index"":""Raven/DocumentsByEntityName"",""id"":""AAAAAAAAAAEAAAAAAAAABQ=="",""time"":""\/Date(1290420997504)\/"",""type"":""Raven.Database.Tasks.RemoveFromIndexTask"",""mergable"":true},""TableId"":9,""TxId"":""wM3q3VA0XkWecl5WBr9Cfw==""}"
				,
				@"{""Cmd"":""Commit"",""TableId"":9,""TxId"":""wM3q3VA0XkWecl5WBr9Cfw==""}",
			};

			var tableStorage = new TableStorage(new MemoryPersistentSource());

			foreach (var cmdText in cmds)
			{
				var command = RavenJObject.Parse(cmdText);
				var tblId = command.Value<int>("TableId");

				var table = tableStorage.Tables[tblId];

				var txId = new Guid(Convert.FromBase64String(command.Value<string>("TxId")));

				var key = command["Key"] as RavenJObject;
				if (key != null)
				{
					foreach (var property in key.ToArray())// nothing in .NET supports iterating & modifying at the same time, no news here
					{
						if(property.Value.Type != JTokenType.String)
							continue;
						var value = property.Value.Value<string>();
						if (value.EndsWith("==") == false)
							continue;

						key[property.Key] = Convert.FromBase64String(value);
					}
				}

				switch (command.Value<string>("Cmd"))
				{
					case "Put":
						table.Put(command["Key"], new byte[] {1, 2, 3}, txId);
						break;
					case "Del":
						table.Remove(command["Key"], txId);
						break;
					case "Commit":
						table.CompleteCommit(txId);
						break;
				}
			}

			Assert.Empty(tableStorage.Tasks);
			Assert.Null(tableStorage.Tasks["ByIndexAndTime"].LastOrDefault());
		}

	    [Fact]
		public void CanAddAndRemoveMultipleTasks_InSingleTx()
		{
			db.TransactionalStorage.Batch(actions=>
			{
				for (int i = 0; i < 3; i++)
				{
					actions.Tasks.AddTask(new RemoveFromIndexTask
					{
						Index = "foo",
						Keys = { "tasks/"+i },
					},SystemTime.Now);
				}
			});

			db.TransactionalStorage.Batch(actions => actions.Tasks.GetMergedTask<RemoveFromIndexTask>());


			db.TransactionalStorage.Batch(actions =>
			{
				var isIndexStale = actions.Staleness.IsIndexStale("foo", null, null);
				Assert.False(isIndexStale);
			});
		}

		[Fact]
		public void CanGetDocumentCounts()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(0, actions.Documents.GetDocumentsCount());

				actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
			});

			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(1, actions.Documents.GetDocumentsCount());

				RavenJObject metadata;
				actions.Documents.DeleteDocument("a", null, out metadata);
			});


			db.TransactionalStorage.Batch(actions => Assert.Equal(0, actions.Documents.GetDocumentsCount()));
		}

		[Fact]
		public void CanGetDocumentAfterEmptyEtag()
		{
			db.TransactionalStorage.Batch(actions => actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject()));

			db.TransactionalStorage.Batch(actions =>
			{
				var documents = actions.Documents.GetDocumentsAfter(Guid.Empty,5).ToArray();
				Assert.Equal(1, documents.Length);
			});
		}

		[Fact]
		public void CanGetDocumentAfterAnEtag()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
				actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
				actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
			});

			db.TransactionalStorage.Batch(actions =>
			{
				var doc = actions.Documents.DocumentByKey("a",null);
				var documents = actions.Documents.GetDocumentsAfter(doc.Etag.Value, 5).Select(x => x.Key).ToArray();
				Assert.Equal(2, documents.Length);
				Assert.Equal("b", documents[0]);
				Assert.Equal("c", documents[1]);
			});
		}

		[Fact]
		public void CanGetDocumentAfterAnEtagAfterDocumentUpdateWouldReturnThatDocument()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
				actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
				actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
			});

			Guid guid = Guid.Empty;
			db.TransactionalStorage.Batch(actions =>
			{
				var doc = actions.Documents.DocumentByKey("a", null);
				guid = doc.Etag.Value;
				actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
			});

			db.TransactionalStorage.Batch(actions =>
			{
				var documents = actions.Documents.GetDocumentsAfter(guid, 5).Select(x => x.Key).ToArray();
				Assert.Equal(3, documents.Length);
				Assert.Equal("b", documents[0]);
				Assert.Equal("c", documents[1]);
				Assert.Equal("a", documents[2]);
			});
		}

		[Fact]
		public void GetDocumentAfterAnEtagWhileAddingDocsFromMultipleThreadsEnumeratesAllDocs()
		{
			var numberOfDocsAdded = 0;
			var threads = new List<Thread>();
			try
			{
				for (var i = 0; i < 10; i++)
				{
					var thread = new Thread(() =>
					{
						var cmds = new List<ICommandData>();
						 for (var k = 0; k < 100; k++)
						 {
							var newId = Interlocked.Increment(ref numberOfDocsAdded);
							cmds.Add(new PutCommandData
							{
								Document = new RavenJObject(),
								Metadata = new RavenJObject(),
								Key = newId.ToString()
							});
						};
						db.Batch(cmds);
					});
					threads.Add(thread);
					thread.Start();
				}

				var docs = new List<string>();
				var lastEtag = Guid.Empty;
				var total = 0;
			    var stop = false;
			    do
			    {
					var etag = lastEtag;
			        var jsonDocuments = new JsonDocument[0];
			        db.TransactionalStorage.Batch(actions =>
			        {
						jsonDocuments = actions.Documents.GetDocumentsAfter(etag, 1000).Where(x => x != null).ToArray();
			        });
					docs.AddRange(jsonDocuments.Select(x=>x.Key));
			        total += jsonDocuments.Length;
			    	if (jsonDocuments.Length > 0)
			    		lastEtag = jsonDocuments.Last().Etag.Value;
			    	if (stop)
						break;
					if (threads.All(x => !x.IsAlive))
						stop = true;
			    } while (true);

				Assert.Equal(numberOfDocsAdded, total);
			}
			finally
			{
				foreach (var thread in threads)
				{
					thread.Join();
				}
			}
		}

		[Fact]
		public void UpdatingDocumentWillKeepSameCount()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(0, actions.Documents.GetDocumentsCount());

				actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());

			});

			db.TransactionalStorage.Batch(actions =>
			{
				Assert.Equal(1, actions.Documents.GetDocumentsCount());

				actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
			});


			db.TransactionalStorage.Batch(actions => Assert.Equal(1, actions.Documents.GetDocumentsCount()));
		}


		[Fact]
		public void CanEnqueueAndPeek()
		{
			db.TransactionalStorage.Batch(actions => actions.Queue.EnqueueToQueue("ayende", new byte[]{1,2}));

			db.TransactionalStorage.Batch(actions => Assert.Equal(new byte[] { 1, 2 }, actions.Queue.PeekFromQueue("ayende").First().Item1));
		}

		[Fact]
		public void PoisonMessagesWillBeDeleted()
		{
			db.TransactionalStorage.Batch(actions => actions.Queue.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

			db.TransactionalStorage.Batch(actions =>
			{
				for (int i = 0; i < 6; i++)
				{
					actions.Queue.PeekFromQueue("ayende").First();
				}
				Assert.Equal(null, actions.Queue.PeekFromQueue("ayende").FirstOrDefault());
			});
		}

		[Fact]
		public void CanDeleteQueuedData()
		{
			db.TransactionalStorage.Batch(actions => actions.Queue.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

			db.TransactionalStorage.Batch(actions =>
			{
				actions.Queue.DeleteFromQueue("ayende", actions.Queue.PeekFromQueue("ayende").First().Item2);
				Assert.Equal(null, actions.Queue.PeekFromQueue("ayende").FirstOrDefault());
			});
		}

		[Fact]
		public void CanGetNewIdentityValues()
		{
			db.TransactionalStorage.Batch(actions=>
			{
				var nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(1, nextIdentityValue);

				nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(2, nextIdentityValue);

			});

			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(3, nextIdentityValue);

				nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(4, nextIdentityValue);

			});
		}

		[Fact]
		public void CanGetNewIdentityValuesWhenUsingTwoDifferentItems()
		{
			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(1, nextIdentityValue);

				nextIdentityValue = actions.General.GetNextIdentityValue("blogs");

				Assert.Equal(1, nextIdentityValue);

			});

			db.TransactionalStorage.Batch(actions =>
			{
				var nextIdentityValue = actions.General.GetNextIdentityValue("blogs");

				Assert.Equal(2, nextIdentityValue);

				nextIdentityValue = actions.General.GetNextIdentityValue("users");

				Assert.Equal(2, nextIdentityValue);

			});
		}
	}
}
