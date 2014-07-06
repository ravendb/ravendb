// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2273.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2273 : ReplicationBase
	{
		[Fact]
		public void CanEnableReplicationOnExistingDatabase()
		{
			var path1 = NewDataPath();
			var path2 = NewDataPath();

			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				CreateDatabase(store, "DB1", path1, "PeriodicExports");
				CreateDatabase(store, "DB2", path2, "PeriodicExports;Replication");

				using (var session = store.OpenSession("DB1"))
				{
					session.Store(new Person { Name = "Person1" });
					session.Store(new Person { Name = "Person2" });
					session.Store(new Person { Name = "Person3" });

					session.SaveChanges();
				}

				CreateDatabase(store, "DB1", path1, "PeriodicExports;Replication");

				SetupReplication(store, "DB1", "DB2");

				WaitForReplication(store, "people/1", "DB2");

				using (var session = store.OpenSession("DB2"))
				{
					var person1 = session.Load<Person>("people/1");
					var metadata1 = session.Advanced.GetMetadataFor(person1);

					Assert.Equal(0, metadata1[Constants.RavenReplicationVersion].Value<long>());
					Assert.Equal(0, metadata1.Value<RavenJArray>(Constants.RavenReplicationHistory).Length);
					Assert.NotNull(metadata1[Constants.RavenReplicationSource]);
				}

				using (var session = store.OpenSession("DB1"))
				{
					var person1 = session.Load<Person>("people/1");
					person1.Name = "Person11";

					session.Store(person1);
					session.SaveChanges();
				}

				WaitForReplication(store, session => session.Load<Person>("people/1").Name == "Person11", "DB2");

				using (var session = store.OpenSession("DB2"))
				{
					var person1 = session.Load<Person>("people/1");
					var metadata1 = session.Advanced.GetMetadataFor(person1);

					Assert.True(metadata1[Constants.RavenReplicationVersion].Value<long>() > 0);

					var history = metadata1.Value<RavenJArray>(Constants.RavenReplicationHistory);
					Assert.Equal(1, history.Length);
					Assert.Equal(0, history[0].Value<long>(Constants.RavenReplicationVersion));

					Assert.NotNull(metadata1[Constants.RavenReplicationSource]);

					person1.Name = "Person111";
					session.Store(person1);
					session.SaveChanges();
				}

				using (var session = store.OpenSession("DB1"))
				{
					var person1 = session.Load<Person>("people/1");
					person1.Name = "Person1111";

					session.Store(person1);
					session.SaveChanges();
				}

				Assert.Throws<ConflictException>(() => WaitForReplication(store, session => session.Load<Person>("people/1").Name == "Person1111", "DB2"));

				SetupReplication(store, "DB2", "DB1");

				using (var session = store.OpenSession("DB2"))
				{
					var person1 = session.Load<Person>("people/2");
					person1.Name = "Person22";

					session.Store(person1);
					session.SaveChanges();
				}

				WaitForReplication(store, session => session.Load<Person>("people/2").Name == "Person22", "DB1");

				using (var session = store.OpenSession("DB1"))
				{
					var person2 = session.Load<Person>("people/2");
					var metadata2 = session.Advanced.GetMetadataFor(person2);

					Assert.True(metadata2[Constants.RavenReplicationVersion].Value<long>() > 0);

					var history = metadata2.Value<RavenJArray>(Constants.RavenReplicationHistory);
					Assert.Equal(1, history.Length);
					Assert.Equal(0, history[0].Value<long>(Constants.RavenReplicationVersion));

					Assert.NotNull(metadata2[Constants.RavenReplicationSource]);
				}
			}
		}

		private static void CreateDatabase(IDocumentStore store, string databaseName, string path, string activeBundles)
		{
			store
				.DatabaseCommands
				.ForSystemDatabase()
				.Put(
					"Raven/Databases/" + databaseName,
					null,
					RavenJObject.FromObject(new DatabaseDocument
					{
						Id = databaseName,
						Settings =
												{
													{ "Raven/DataDir", path }, 
													{ "Raven/ActiveBundles", activeBundles },
												}
					}),
					new RavenJObject());
		}

		private static void SetupReplication(IDocumentStore store, string sourceDatabaseName, string destinationDatabaseName)
		{
			store
				.DatabaseCommands
				.ForDatabase(sourceDatabaseName)
				.Put(
					Constants.RavenReplicationDestinations,
					null,
					RavenJObject.FromObject(new ReplicationDocument
					{
						Destinations = new List<ReplicationDestination>
					                                            {
						                                            new ReplicationDestination
						                                            {
							                                            Database = destinationDatabaseName,
																		Url = store.Url
						                                            }
					                                            }
					}),
					new RavenJObject());
		}
	}
}