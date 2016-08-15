using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Replication;
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
		public void All_remote_etags_lower_than_local_should_return_RequireUpdate_at_conflict_status()
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
		public void Remote_change_vector_larger_than_local_should_return_RequireUpdate_at_conflict_status()
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
		public void Remote_change_vector_smaller_than_local_should_return_Conflict_at_conflict_status()
		{
			var dbIds = new List<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

			var local = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 40 }
			};

			var remote = new[]
			{
				new ChangeVectorEntry { DbId = dbIds[0], Etag = 10 },
				new ChangeVectorEntry { DbId = dbIds[1], Etag = 20 },
				new ChangeVectorEntry { DbId = dbIds[2], Etag = 30 }
			};

			Assert.Equal(IncomingReplicationHandler.ConflictStatus.Conflict, IncomingReplicationHandler.GetConflictStatus(remote, local));
		}

		[Fact(Skip = "conflict code is not yet finished")]
		public async Task Conflict_should_occur_after_changes_to_the_same_document_on_two_nodes_at_the_same_time()
		{
			var dbName1 = DbName + "-1";
			var dbName2 = DbName + "-2";
			var dbName3 = DbName + "-3";
			using (var store1 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName1))
			using (var store2 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName2))
			using (var store3 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName3))
			{
				store1.DefaultDatabase = dbName1;
				store2.DefaultDatabase = dbName2;
				store3.DefaultDatabase = dbName2;

				//TODO : finish

				SetupReplication(dbName2, store1, store2);
				SetupReplication(dbName2, store1, store3);
				SetupReplication(dbName1, store2, store1);
				SetupReplication(dbName1, store2, store3);
				SetupReplication(dbName3, store3, store1);
				SetupReplication(dbName3, store3, store2);
			}
		}
	}
}
