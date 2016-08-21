using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Replication.Messages;
using Raven.Json.Linq;
using Raven.Server.Documents.Replication;
using Raven.Server.Extensions;
using Voron.Tests;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationConflictsTests : ReplicationTestsBase
    {
		public readonly string DbName = "TestDB" + Guid.NewGuid();

		public class User
		{
			public string Name { get; set; }
			public int Age { get; set; }
		}

		[Fact]
		public void All_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
		{
			var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 11 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 12 },
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.AlreadyMerged, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}

		[Fact]
		public void All_local_etags_lower_than_remote_should_return_Update_at_conflict_status()
		{
			var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.Update, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}

		[Fact]
		public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status()
		{
			var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 75 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 95 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 2 },
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}

		[Fact]
		public void Some_remote_etags_lower_than_local_and_some_higher_should_return_Conflict_at_conflict_status_with_different_order()
		{
			var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 75 },
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 },
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 95 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 2 },
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}

		[Fact]
		public void Remote_change_vector_larger_size_than_local_should_return_Update_at_conflict_status()
		{
			var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 40 }
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.Update, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}

	    [Fact]
	    public void Remote_change_vector_with_different_dbId_set_than_local_should_return_Conflict_at_conflict_status()
	    {
		    var dbIds = new List<Guid> {Guid.NewGuid(), Guid.NewGuid()};
			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 10 }
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}

		[Fact]
		public void Remote_change_vector_smaller_than_local_and_all_remote_etags_lower_than_local_should_return_AlreadyMerged_at_conflict_status()
		{
			var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
				new ChangeVectorEntry { DbId = dbIds[3], Etag = 40 }
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 1 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 2 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 3 }
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.AlreadyMerged, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}

		[Fact]
		public void Remote_change_vector_smaller_than_local_and_some_remote_etags_higher_than_local_should_return_Conflict_at_conflict_status()
		{
			var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 3000 },
				new ChangeVectorEntry { DbId = dbIds[3], Etag = 40 }
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 100 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 200 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 300 }
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}


		[Fact]
		public async Task Conflict_should_occur_after_changes_to_the_same_document_on_two_nodes_at_the_same_time_with_master_slave()
		{
			var dbName1 = DbName + "-1";
			var dbName2 = DbName + "-2";
			using (var store1 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName1))
			using (var store2 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName2))
			{
				store1.DefaultDatabase = dbName1;
				store2.DefaultDatabase = dbName2;
				store1.DatabaseCommands.ForDatabase(dbName1).Put("foo/bar", null, new RavenJObject(), new RavenJObject());
				store2.DatabaseCommands.ForDatabase(dbName2).Put("foo/bar", null, new RavenJObject(), new RavenJObject());

				SetupReplication(dbName2, store1, store2);

				var conflicts = await WaitUntilHasConflict(dbName2, store2, "foo/bar");
				Assert.Equal(2, conflicts["foo/bar"].Count);
			}
		}

		

		[Fact]
		public async Task Conflict_should_occur_after_changes_to_the_same_document_on_two_nodes_at_the_same_time_with_master_master()
		{
			var dbName1 = DbName + "-1";
			var dbName2 = DbName + "-2";
			using (var store1 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName1))
			using (var store2 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName2))
			{
				store1.DefaultDatabase = dbName1;
				store2.DefaultDatabase = dbName2;
				store1.DatabaseCommands.ForDatabase(dbName1).Put("foo/bar", null, new RavenJObject(), new RavenJObject());
				store2.DatabaseCommands.ForDatabase(dbName2).Put("foo/bar", null, new RavenJObject(), new RavenJObject());

				SetupReplication(dbName1, store2, store1);
				SetupReplication(dbName2, store1, store2);
				
				var conflicts1 = await WaitUntilHasConflict(dbName1, store1, "foo/bar");
				var conflicts2 = await WaitUntilHasConflict(dbName2, store2, "foo/bar");

				Assert.Equal(2, conflicts1["foo/bar"].Count);
				Assert.Equal(2, conflicts2["foo/bar"].Count);
			}
		}

		private async Task<Dictionary<string, List<ChangeVectorEntry[]>>>
			WaitUntilHasConflict(string dbName, DocumentStore store, string docId, int timeout = 10000)
		{
			bool hasConflict;
			Dictionary<string, List<ChangeVectorEntry[]>> conflicts;
			var sw = Stopwatch.StartNew();
			do
			{
				conflicts = await GetConflicts(store, dbName, docId);
				hasConflict = conflicts.Count > 0;
				if (hasConflict == false && sw.ElapsedMilliseconds > timeout)
					Assert.False(true, "Timed out while waiting for conflicts");
			} while (hasConflict == false);
			return conflicts;
		}

		private async Task<Dictionary<string, List<ChangeVectorEntry[]>>> GetConflicts(DocumentStore store, string dbName,
		    string docId)
	    {
			var url = $"{store.Url}/databases/{dbName}/replication/conflicts?docId={docId}";
			using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
			{
				request.ExecuteRequest();
				var conflictsJson = RavenJArray.Parse(await request.Response.Content.ReadAsStringAsync());
				var conflicts = conflictsJson.Select(x => new
				{
					Key = x.Value<string>("Key"),
					ChangeVector = x.Value<RavenJArray>("ChangeVector").Select(c => c.FromJson()).ToArray()
				}).GroupBy(x => x.Key).ToDictionary(x => x.Key,x => x.Select(i => i.ChangeVector).ToList());

				return conflicts;
			}
		}


		private async Task<bool> HasConflicts(DocumentStore store, string dbName,string docId)
	    {
		    var url = $"{store.Url}/databases/{dbName}/replication/conflicts?docId={docId}";
			using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(null, url, HttpMethod.Get, new OperationCredentials(null, CredentialCache.DefaultCredentials), new DocumentConvention())))
			{
				request.ExecuteRequest();
				var conflictsJson = RavenJArray.Parse(await request.Response.Content.ReadAsStringAsync());
				return conflictsJson.Length > 0;
			}		    
	    }
	}
}
