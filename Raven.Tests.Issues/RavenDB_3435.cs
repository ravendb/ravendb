using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Jint.Parser.Ast;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Tasks;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Bundles.MoreLikeThis;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3435 : ReplicationBase
	{
		private const string TestDatabaseName = "testDB";
		private const string TestUsername1 = "John Doe A";
		private const string TestUsername2 = "John Doe B";

		public class User
		{
			public string Id { get; set; }

			public string Name { get; set; }
		}

		public class DocumentStoreSetDateModifiedListener : IDocumentStoreListener
		{
			// This will get called after the 'DocumentConflictListener' is handled - repopulating the Asos
			// Last Modified Date property
			public bool BeforeStore(string key, object entityInstance, RavenJObject metadata, RavenJObject original)
			{
				if (metadata == null)
				{
					// Log that the metadata was null (warning?), but assume all is well
					return true;
				}

				var lastModifiedDate = DateTime.UtcNow;
				metadata[Constants.RavenLastModified] = lastModifiedDate;
				return true;
			}

			public void AfterStore(string key, object entityInstance, RavenJObject metadata)
			{
				// All's good in the hood
			}
		}

		public class TestDocumentConflictListener : IDocumentConflictListener
		{
			public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
			{
				if (conflictedDocs == null || !conflictedDocs.Any())
				{
					resolvedDocument = null;
					return false;
				}

				if (key.StartsWith("Raven/"))
				{
					resolvedDocument = null;
					return false;
				}

				var maxDate = conflictedDocs.Max(x => x.Metadata.Value<DateTimeOffset>(Constants.RavenLastModified));


				resolvedDocument =
					conflictedDocs.FirstOrDefault(x => x.Metadata.Value<DateTimeOffset>(Constants.RavenLastModified) == maxDate);
				if (resolvedDocument != null)
				{
					// Do the logging before we override the metadata
					resolvedDocument.Metadata.Remove("@id");
					resolvedDocument.Metadata.Remove("@etag");
					resolvedDocument.Metadata.Remove(Constants.RavenReplicationConflict);
					resolvedDocument.Metadata.Remove(Constants.RavenReplicationConflictDocument);
				}

				return resolvedDocument != null;
			}
		}

		[Fact]
		public void Resolution_of_conflict_should_delete_all_conflict_files()
		{
			var user = new User
			{
				Name = TestUsername1
			};

			using (var storeA = CreateStore(useFiddler:true,databaseName:TestDatabaseName, runInMemory:false, configureStore: store => store.RegisterListener(new TestDocumentConflictListener())))
			using (var storeB = CreateStore(useFiddler:true,databaseName: TestDatabaseName, runInMemory: false))
			{				
				storeA.DatabaseCommands.GlobalAdmin.DeleteDatabase(TestDatabaseName);
				storeB.DatabaseCommands.GlobalAdmin.DeleteDatabase(TestDatabaseName);

				storeA.DatabaseCommands.GlobalAdmin.CreateDatabase(MultiDatabase.CreateDatabaseDocument(TestDatabaseName));
				storeB.DatabaseCommands.GlobalAdmin.CreateDatabase(MultiDatabase.CreateDatabaseDocument(TestDatabaseName));

				//setup master-master
				TellFirstInstanceToReplicateToSecondInstance();			
				StoreDataDocWithConflictDocs(user, storeA);

				WaitForReplication(storeB, user.Id);

				TellSecondInstanceToReplicateToFirstInstance();

				PauseAllReplicationTasks();

				StopDatabase(1);

				// Precaution -> if this fails -> something is wrong in how the test is written
				Assert.DoesNotThrow(() => ChangeDocument(storeA, user.Id, TestUsername2));

				StopDatabase(0);
				StartDatabase(1);

				// Precaution -> if this fails -> something is wrong in how the test is written
				Assert.DoesNotThrow(() => ChangeDocument(storeB, user.Id, TestUsername2));
				
				StartDatabase(0);

				ExecuteReplicationOnAllServers();			

				Assert.True(HasConflictDocuments(storeA, user.Id));
				Assert.True(HasConflictDocuments(storeB, user.Id));							

				using (var session = storeA.OpenSession())
					session.Load<User>(user.Id); //resolve conflict on A (resolver listener registered)

				ExecuteReplicationOnAllServers();

				//sanity check
				Assert.False(HasConflictDocuments(storeA, user.Id));
				Assert.False(HasConflictDocuments(storeB, user.Id));

				//storeB has no conflict resolver listeners, but since
				//the conflict was resolved on A, it should be resolved on B as well
				using (var session = storeB.OpenSession())
					Assert.DoesNotThrow(() => session.Load<User>(user.Id));

			}
		}

		private bool HasConflictDocuments(IDocumentStore store, string id)
		{
			var docs = store.DatabaseCommands.ForDatabase(TestDatabaseName).GetDocuments(0, 1024);
			return docs.Any(d => d.Key.Contains(id + "/conflicts"));
		}

		private void PauseAllReplicationTasks()
		{
			Parallel.ForEach(servers, srv =>
			{
				var documentDatabaseTask = srv.Server.GetDatabaseInternal(TestDatabaseName);
				documentDatabaseTask.Wait();
				var replicationTask = documentDatabaseTask.Result.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();

				if (replicationTask != null)
					replicationTask.Pause();
			});
		}

		private void ExecuteReplicationOnAllServers()
		{
			var countdown = new CountdownEvent(servers.Count);
			Parallel.ForEach(servers, srv =>
			{
				var documentDatabaseTask = srv.Server.GetDatabaseInternal(TestDatabaseName);
				documentDatabaseTask.Wait();
				var replicationTask = documentDatabaseTask.Result.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();

				if (replicationTask != null)
				{
					replicationTask.ShouldWaitForWork = false;
					replicationTask.ReplicationExecuted +=  documentDatabaseTask.Result.WorkContext.StopWork;
					var executeMethod = typeof(ReplicationTask).GetMethod("Execute", BindingFlags.NonPublic | BindingFlags.Instance);

					countdown.Signal();
					countdown.Wait(); //make sure that replication will start as simultaneously as possible
		
					executeMethod.Invoke(replicationTask, new object[0]);
					replicationTask.ReplicationExecuted -= documentDatabaseTask.Result.WorkContext.StopWork;
					documentDatabaseTask.Result.WorkContext.StartWork();
				}
			});
		}

		private static void ChangeDocument(DocumentStore store, string id, string newName)
		{
			using (var session = store.OpenSession())
			{
				var fetchedUser = session.Load<User>(id);				
				fetchedUser.Name = newName;
				session.SaveChanges();
			}
		}

		private static void StoreDataDocWithConflictDocs(User user, DocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				session.Store(user);
				session.SaveChanges();
			}
		}
	}
}
