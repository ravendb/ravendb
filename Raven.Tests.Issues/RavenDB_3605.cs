// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3605.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3605 : ReplicationBase
	{
		private class Raven_ConflictDocuments : AbstractIndexCreationTask
		{
			public override string IndexName
			{
				get
				{
					return "Raven/ConflictDocuments";
				}
			}

			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition
				{
					Map = "from doc in docs let id = doc[\"@metadata\"][\"@id\"] where doc[\"@metadata\"][\"Raven-Replication-Conflict\"] == true && (id.Length < 47 || !id.Substring(id.Length - 47).StartsWith(\"/conflicts/\", StringComparison.OrdinalIgnoreCase)) select new { ConflictDetectedAt = (DateTime)doc[\"@metadata\"][\"Last-Modified\"] }",
					Name = "Raven/ConflictDocuments"
				};
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnce1()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToLocal);

				AssertAllConflictsResolved(store2);

				AssertPerson(store2, "people/1", "William");
				AssertPerson(store2, "people/2", "George");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnce2()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToRemote);

				AssertAllConflictsResolved(store2);

				AssertPerson(store2, "people/1", "John");
				AssertPerson(store2, "people/2", "Edward");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnce3()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToLatest);

				AssertAllConflictsResolved(store2);

				AssertPerson(store2, "people/1", "William");
				AssertPerson(store2, "people/2", "George");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceDeletes1()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2, true);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToLocal);

				AssertAllConflictsResolved(store2);

				AssertNullPerson(store2, "people/1");
				AssertNullPerson(store2, "people/2");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceDeletes2()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2, true);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToLatest);

				AssertAllConflictsResolved(store2);

				AssertNullPerson(store2, "people/1");
				AssertNullPerson(store2, "people/2");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceDeletes3()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2, true);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToRemote);

				AssertAllConflictsResolved(store2);

				AssertPerson(store2, "people/1", "John");
				AssertPerson(store2, "people/2", "Edward");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceDeletesInverse1()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2, true, true);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToLocal);

				AssertAllConflictsResolved(store2);

				AssertPerson(store2, "people/1", "William");
				AssertPerson(store2, "people/2", "George");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceDeletesInverse2()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2, true, true);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToLatest);

				AssertAllConflictsResolved(store2);

				AssertPerson(store2, "people/1", "William");
				AssertPerson(store2, "people/2", "George");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceDeletesInverse3()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployAndWaitForConflicts(store1, store2, true, true);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToRemote);

				AssertAllConflictsResolved(store2);

				AssertNullPerson(store2, "people/1");
				AssertNullPerson(store2, "people/2");
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceNorthwind1()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployNorthwindAndWaitForConflicts(store1, store2);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToLocal);

				AssertAllConflictsResolved(store2);
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceNorthwind2()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployNorthwindAndWaitForConflicts(store1, store2);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToLatest);

				AssertAllConflictsResolved(store2);
			}
		}

		[Fact]
		public void CanResolveAllConflictsAtOnceNorthwind3()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployNorthwindAndWaitForConflicts(store1, store2);

				ResolveAllConflictsAndWaitForOperationToComplete(store2, StraightforwardConflictResolution.ResolveToRemote);

				AssertAllConflictsResolved(store2);
			}
		}

		private static void AssertPerson(IDocumentStore store, string id, string name)
		{
			using (var session = store.OpenSession())
			{
				var person = session.Load<Person>(id);
				Assert.NotNull(person);
				Assert.Equal(name, person.Name);
			}
		}

		private static void AssertNullPerson(IDocumentStore store, string id)
		{
			using (var session = store.OpenSession())
			{
				var person = session.Load<Person>(id);
				Assert.Null(person);
			}
		}

		private static void AssertAllConflictsResolved(IDocumentStore store2)
		{
			var result = store2.DatabaseCommands.Query(new Raven_ConflictDocuments().IndexName, new IndexQuery());
			Assert.Equal(0, result.TotalResults);
			Assert.False(result.IsStale);
		}

		private static void ResolveAllConflictsAndWaitForOperationToComplete(DocumentStore store, StraightforwardConflictResolution resolution)
		{
			var request = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store.Url.ForDatabase(store.DefaultDatabase) + "/studio-tasks/replication/conflicts/resolve?resolution=" + resolution, HttpMethod.Post, store.DatabaseCommands.PrimaryCredentials, store.Conventions));
			var response = request.ReadResponseJson();

			var operationId = response.Value<long>("OperationId");
			var client = (AsyncServerClient)store.AsyncDatabaseCommands;

			var operation = new Operation(client, operationId);
			operation.WaitForCompletion();

			WaitForIndexing(store);
		}

		private void DeployNorthwindAndWaitForConflicts(DocumentStore store1, DocumentStore store2)
		{
			DeployNorthwind(store1);
			DeployNorthwind(store2);

			WaitForIndexing(store1);
			WaitForIndexing(store2);

			TellFirstInstanceToReplicateToSecondInstance();

			WaitForReplication(store2, session =>
			{
				try
				{
					session.Load<dynamic>("orders/1");
					return false;
				}
				catch (ConflictException)
				{
					return true;
				}
			});

			new Raven_ConflictDocuments().Execute(store2);

			WaitForIndexing(store2);
		}

		private void DeployAndWaitForConflicts(IDocumentStore store1, IDocumentStore store2, bool conflictsOnDelete = false, bool inverseDeletes = false)
		{
			using (var session = store1.OpenSession())
			{
				session.Store(new Person { Name = "John" });
				session.Store(new Person { Name = "Edward" });

				session.SaveChanges();

				if (conflictsOnDelete && inverseDeletes)
				{
					session.Delete("people/1");
					session.Delete("people/2");

					session.SaveChanges();
				}
			}

			using (var session = store2.OpenSession())
			{
				session.Store(new Person { Name = "William" });
				session.Store(new Person { Name = "George" });

				session.SaveChanges();

				if (conflictsOnDelete && inverseDeletes == false)
				{
					session.Delete("people/1");
					session.Delete("people/2");

					session.SaveChanges();
				}
			}

			WaitForIndexing(store1);
			WaitForIndexing(store2);

			TellFirstInstanceToReplicateToSecondInstance();

			WaitForReplication(store2, session =>
			{
				try
				{
					session.Load<dynamic>("people/1");
					return false;
				}
				catch (ConflictException)
				{
					return true;
				}
			});

			new Raven_ConflictDocuments().Execute(store2);

			WaitForIndexing(store2);
		}
	}
}