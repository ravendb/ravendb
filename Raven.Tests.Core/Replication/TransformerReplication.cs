using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Core.Replication
{
	public class TransformerReplication : RavenTestBase
	{
		public class UserWithExtraInfo
		{
			public string Id { get; set; }

			public string Name { get; set; }

			public string Address { get; set; }
		}

		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class UserWithoutExtraInfoTransformer : AbstractTransformerCreationTask<UserWithExtraInfo>
		{
			public UserWithoutExtraInfoTransformer()
			{
				TransformResults = usersWithExtraInfo => from u in usersWithExtraInfo
					select new
					{
						u.Id,
						u.Name
					};
			}
		}

		[Fact]
		public void Should_replicate_transformers_if_convention_is_set()
		{
			var requestFactory = new HttpRavenRequestFactory();
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
			{
				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.Transformers;
				// ReSharper disable once AccessToDisposedClosure
				SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

				var transformer = new UserWithoutExtraInfoTransformer();
				transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

				var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

				var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination2.Count(x => x.Name == transformer.TransformerName));
				
				var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));

			}
		}

		private static void CreateDatabaseWithReplication(DocumentStore store, string databaseName)
		{
			store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = databaseName,
				Settings =
				{
					{"Raven/DataDir", "~/Tenants/" + databaseName},
					{"Raven/ActiveBundles", "Replication"}
				}
			});
		}

		private static void SetupReplication(IDocumentStore source, string databaseName, Func<IDocumentStore, bool> shouldSkipIndexReplication, params IDocumentStore[] destinations)
		{
			var replicationDocument = new ReplicationDocument
			{
				Destinations = new List<ReplicationDestination>(destinations.Select(destination =>
					new ReplicationDestination
					{
						Database = databaseName,
						Url = destination.Url,
						SkipIndexReplication = shouldSkipIndexReplication(destination)
					}))

			};

			using (var session = source.OpenSession(databaseName))
			{
				session.Store(replicationDocument, Constants.RavenReplicationDestinations);
				session.SaveChanges();
			}
		}
	}
}
