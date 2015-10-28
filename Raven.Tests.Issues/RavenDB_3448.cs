// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3448.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Client.Exceptions;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Remote;
using Raven.Smuggler.Database.Streams;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3448 : ReplicationBase
	{
		private class Person
		{
			public string Id { get; set; }

			public string Name { get; set; }
		}

		[Fact]
		public async Task SmugglerShouldImportConflictsProperly()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				using (var session = store1.OpenSession())
				{
					session.Store(new Person());
					session.SaveChanges();
				}

				using (var session = store2.OpenSession())
				{
					session.Store(new Person());
					session.SaveChanges();
				}

				WaitForIndexing(store1);
				WaitForIndexing(store2);

				TellFirstInstanceToReplicateToSecondInstance();

				WaitForReplication(store2, session =>
				{
					try
					{
						session.Load<Person>("people/1");
						return false;
					}
					catch (ConflictException)
					{
						return true;
					}
				});

				store2.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("Northwind");

				using (var stream = new MemoryStream())
				{
				    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(), 
                        new DatabaseSmugglerRemoteSource(
                            new DatabaseSmugglerRemoteConnectionOptions
                            {
                                Url = store2.Url,
                                Database = store2.DefaultDatabase
                            }),
                        new DatabaseSmugglerStreamDestination(stream));

				    await smuggler.ExecuteAsync();

				    stream.Position = 0;

				    smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(), 
                        new DatabaseSmugglerStreamSource(stream), 
                        new DatabaseSmugglerRemoteDestination(
                            new DatabaseSmugglerRemoteConnectionOptions
                            {
                                Database = "Northwind",
                                Url = store2.Url
                            }));

                    await smuggler.ExecuteAsync();
				}

				Assert.Throws<ConflictException>(() =>
				{
					using (var session = store2.OpenSession("Northwind"))
					{
						session.Load<Person>("people/1");
					}
				});
			}
		}
	}
}