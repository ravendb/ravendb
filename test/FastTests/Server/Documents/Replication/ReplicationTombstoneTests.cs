using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationTombstoneTests : ReplicationTestsBase
    {
		[Fact]
		public async Task Tombstones_replication_should_delete_document_at_destination()
		{
			var dbName1 = "FooBar-1";
			var dbName2 = "FooBar-2";
			using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
			using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
			{
				using (var s1 = store1.OpenSession())
				{
					s1.Store(new ReplicationConflictsTests.User(), "foo/bar");
					s1.SaveChanges();
				}

				SetupReplication(store1, store2);

				Assert.True(WaitForDocument(store2, "foo/bar"));

				using (var s1 = store1.OpenSession())
				{
					s1.Delete("foo/bar");
					s1.SaveChanges();
				}

				var tombstoneIDs = await WaitUntilHasTombstones(store2);
				Assert.Equal(1, tombstoneIDs.Count);
				Assert.Contains("foo/bar", tombstoneIDs);

				Assert.False(WaitForDocument(store2, "foo/bar", 1000));
			}
		}

		[Fact]
		public async Task Tombstones_replication_should_delete_document_at_multiple_destinations_fan()
		{
			var dbName1 = "FooBar-1";
			var dbName2 = "FooBar-2";
			var dbName3 = "FooBar-3";
			using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
			using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
			using (var store3 = GetDocumentStore(dbSuffixIdentifier: dbName3))
			{
				using (var s1 = store1.OpenSession())
				{
					s1.Store(new ReplicationConflictsTests.User(), "foo/bar");
					s1.SaveChanges();
				}

				SetupReplication(store1, store2, store3);

				Assert.True(WaitForDocument(store2, "foo/bar"),store2.Identifier);
				Assert.True(WaitForDocument(store3, "foo/bar"),store3.Identifier);

				using (var s1 = store1.OpenSession())
				{
					s1.Delete("foo/bar");
					s1.SaveChanges();
				}

				var tombstoneIDs = await WaitUntilHasTombstones(store2);
				Assert.Equal(1, tombstoneIDs.Count);
				Assert.Contains("foo/bar", tombstoneIDs);

				Assert.True(WaitForDocumentDeletion(store2, "foo/bar", 1000));

				tombstoneIDs = await WaitUntilHasTombstones(store3);
				Assert.Equal(1, tombstoneIDs.Count);
				Assert.Contains("foo/bar", tombstoneIDs);

				Assert.True(WaitForDocumentDeletion(store3, "foo/bar", 1000));
			}
		}

		[Fact]
		public async Task Tombstones_replication_should_delete_document_at_multiple_destinations_chain()
		{
			var dbName1 = "FooBar-1";
			var dbName2 = "FooBar-2";
			var dbName3 = "FooBar-3";
			using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
			using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
			using (var store3 = GetDocumentStore(dbSuffixIdentifier: dbName3))
			{
				using (var s1 = store1.OpenSession())
				{
					s1.Store(new ReplicationConflictsTests.User(), "foo/bar");
					s1.SaveChanges();
				}

				SetupReplication(store1, store2);
				SetupReplication(store2, store3);

				Assert.True(WaitForDocument(store2, "foo/bar"), store2.Identifier);
				Assert.True(WaitForDocument(store3, "foo/bar"), store3.Identifier);

				using (var s1 = store1.OpenSession())
				{
					s1.Delete("foo/bar");
					s1.SaveChanges();
				}

				var tombstoneIDs = await WaitUntilHasTombstones(store2);
				Assert.Equal(1, tombstoneIDs.Count);
				Assert.Contains("foo/bar", tombstoneIDs);

				Assert.True(WaitForDocumentDeletion(store2, "foo/bar", 100));
				
				tombstoneIDs = await WaitUntilHasTombstones(store3);
				Assert.Equal(1, tombstoneIDs.Count);
				Assert.Contains("foo/bar", tombstoneIDs);

				Assert.True(WaitForDocumentDeletion(store3, "foo/bar", 100));
			}
		}

	    [Fact]
	    public async Task Tombstone_should_replicate_in_master_master()
	    {
			var dbName1 = "FooBar-1";
			var dbName2 = "FooBar-2";
			using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
			using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
			{
				using (var s1 = store1.OpenSession())
				{
					s1.Store(new ReplicationConflictsTests.User(), "foo/bar");
					s1.SaveChanges();
				}

				SetupReplication(store1, store2);
				SetupReplication(store2, store1);

				Assert.True(WaitForDocument(store2, "foo/bar"));

				using (var s2 = store1.OpenSession())
				{
					s2.Delete("foo/bar");
					s2.SaveChanges();
				}

				var tombstoneIDs = await WaitUntilHasTombstones(store1);
				Assert.Equal(1, tombstoneIDs.Count);
				Assert.Contains("foo/bar", tombstoneIDs);

				Assert.False(WaitForDocument(store1, "foo/bar", 1000));
			}
		}

		//[Fact]
		//public async Task Tombstone_should_replicate_in_master_master_cycle()
		//{
		//	var dbName1 = "FooBar-1";
		//	var dbName2 = "FooBar-2";
		//	var dbName3 = "FooBar-3";
		//	using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
		//	using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
		//	using (var store3 = GetDocumentStore(dbSuffixIdentifier: dbName3))
		//	{
		//		using (var s1 = store1.OpenSession())
		//		{
		//			s1.Store(new ReplicationConflictsTests.User(), "foo/bar");
		//			s1.SaveChanges();
		//		}

		//		using (var s2 = store1.OpenSession())
		//		{
		//			s2.Store(new ReplicationConflictsTests.User(), "foo/bar2");
		//			s2.SaveChanges();
		//		}

		//		SetupReplication(store1, store2);
		//		SetupReplication(store2, store1);
		//		SetupReplication(store2, store3);
		//		SetupReplication(store3, store2);
		//		SetupReplication(store3, store1);
		//		SetupReplication(store1, store3);

		//		Assert.True(WaitForDocument(store1, "foo/bar"));
		//		Assert.True(WaitForDocument(store2, "foo/bar"));
		//		Assert.True(WaitForDocument(store3, "foo/bar"));

		//		Assert.True(WaitForDocument(store1, "foo/bar2"));
		//		Assert.True(WaitForDocument(store2, "foo/bar2"));
		//		Assert.True(WaitForDocument(store3, "foo/bar2"));

		//		using (var s2 = store1.OpenSession())
		//		{
		//			s2.Delete("foo/bar");
		//			s2.SaveChanges();
		//		}

		//		using (var s3 = store1.OpenSession())
		//		{
		//			s3.Delete("foo/bar2");
		//			s3.SaveChanges();
		//		}

		//		var tombstoneIDs = await WaitUntilHasTombstones(store1,2);
		//		Assert.Equal(2, tombstoneIDs.Count);
		//		Assert.Contains("foo/bar", tombstoneIDs);
		//		Assert.Contains("foo/bar2", tombstoneIDs);

		//		tombstoneIDs = await WaitUntilHasTombstones(store2, 2);
		//		Assert.Equal(2, tombstoneIDs.Count);
		//		Assert.Contains("foo/bar", tombstoneIDs);
		//		Assert.Contains("foo/bar2", tombstoneIDs);

		//		tombstoneIDs = await WaitUntilHasTombstones(store3, 2);
		//		Assert.Equal(2, tombstoneIDs.Count);
		//		Assert.Contains("foo/bar", tombstoneIDs);
		//		Assert.Contains("foo/bar2", tombstoneIDs);
		//	}
		//}

		[Fact]
		public async Task Replication_of_document_should_delete_existing_tombstone_at_destination()
		{
			var dbName1 = "FooBar-1";
			var dbName2 = "FooBar-2";
			using (var store1 = GetDocumentStore(dbSuffixIdentifier: dbName1))
			using (var store2 = GetDocumentStore(dbSuffixIdentifier: dbName2))
			{
				using (var s1 = store1.OpenSession())
				{
					s1.Store(new ReplicationConflictsTests.User(), "foo/bar");
					s1.SaveChanges();
				}

				SetupReplication(store1, store2);

				Assert.True(WaitForDocument(store2, "foo/bar"));

				using (var s1 = store1.OpenSession())
				{
					s1.Delete("foo/bar");
					s1.SaveChanges();
				}

				var tombstoneIDs = await WaitUntilHasTombstones(store2);
				Assert.Equal(1, tombstoneIDs.Count);
				Assert.Contains("foo/bar", tombstoneIDs);

				using (var s1 = store1.OpenSession())
				{
					s1.Store(new ReplicationConflictsTests.User(), "foo/bar");
					s1.SaveChanges();
				}

				//first wait until everything is replicated
				Assert.True(WaitForDocument(store1, "foo/bar"));
				Assert.True(WaitForDocument(store2, "foo/bar"));

				//then verify that tombstone is deleted
				var tombstonesAtStore2 = await GetTombstones(store2);
				Assert.Empty(tombstonesAtStore2);
			}
		}
	}
}
