//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Sample.Replication
{
	using System;
	using System.Collections.Generic;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Replication;
	using Raven.Client;
	using Raven.Client.Document;
	using Raven.Client.Exceptions;

	public class Program
	{
		private const string Url1 = "http://localhost:8079";

		private const string Url2 = "http://localhost:8078";

		private const string DatabaseName = "ReplicationSampleDB";

		static void Main()
		{
			var store1 = new DocumentStore
							 {
								 Url = Url1
							 };

			var store2 = new DocumentStore
							 {
								 Url = Url2
							 };

			store1.Initialize();
			store2.Initialize();

			CreateDatabase(store1, DatabaseName);
			Console.WriteLine("Created {0} database on {1}.", DatabaseName, store1.Url);
			CreateDatabase(store2, DatabaseName);
			Console.WriteLine("Created {0} database on {1}.", DatabaseName, store2.Url);

			using (var session1 = store1.OpenSession(DatabaseName))
			{
				session1.Store(new User { Id = "users/ayende", Name = "Ayende" });
				session1.SaveChanges();
			}

			Console.WriteLine("Created document users/ayende on {0}.", store1.Url);

			using (var session2 = store2.OpenSession(DatabaseName))
			{
				session2.Store(new User { Id = "users/ayende", Name = "Oren" });
				session2.SaveChanges();
			}

			Console.WriteLine("Created document users/ayende on {0}.", store2.Url);

			CreateReplication(store1, DatabaseName, Url2, DatabaseName);
			Console.WriteLine("Created Replication document on {0}.", store1.Url);

			Console.WriteLine("Press any key to continue...");
			Console.ReadLine();

			using (var session2 = store2.OpenSession(DatabaseName))
			{
				try
				{
					session2.Load<User>("users/ayende");
				}
				catch (ConflictException e)
				{
					Console.WriteLine("Found conflict in users/ayende on {0}. Choose which document you want to preserve:", Url2);
					var list = new List<JsonDocument>();
					for (int i = 0; i < e.ConflictedVersionIds.Length; i++)
					{
						var doc = store2.DatabaseCommands.ForDatabase(DatabaseName).Get(e.ConflictedVersionIds[i]);
						list.Add(doc);
						Console.WriteLine("{0}. {1}", i, doc.DataAsJson);
					}

					var select = int.Parse(Console.ReadLine());
					var resolved = list[select];
					store2.DatabaseCommands.ForDatabase(DatabaseName).Put("users/ayende", null, resolved.DataAsJson, resolved.Metadata);
				}
			}

			Console.WriteLine("Conflict resolved...");
			Console.WriteLine("Press any key to continue...");
			Console.ReadLine();

			using (var session = store2.OpenSession(DatabaseName))
			{
				var user = session.Load<User>("users/ayende");
				Console.WriteLine(user.Name);
				user.Name = "Ayende Rahien";
				session.SaveChanges();
			}

			store1.Dispose();
			store2.Dispose();
		}

		private static void CreateDatabase(IDocumentStore documentStore, string databaseName)
		{
			using (IDocumentSession session = documentStore.OpenSession())
			{
				var databaseDocument = new DatabaseDocument
				{
					Id = "Raven/Databases/" + databaseName,
					Settings =
						new Dictionary<string, string>
								                       {
									                       {"Raven/DataDir","~\\Databases\\" + databaseName},
									                       {"Raven/ActiveBundles","Replication"}
								                       }
				};

				session.Store(databaseDocument);
				session.SaveChanges();
			}
		}

		private static void CreateReplication(IDocumentStore documentStore, string sourceDatabaseName, string destinationUrl, string destinationDatabaseName)
		{
			using (var session = documentStore.OpenSession(sourceDatabaseName))
			{
				var replicationDocument = new ReplicationDocument
				{
					Destinations = new List<ReplicationDestination>
							                                         {
								                                         new ReplicationDestination
									                                         {
										                                         Url = destinationUrl, 
																				 Database = destinationDatabaseName
									                                         }
							                                         }
				};

				session.Store(replicationDocument);
				session.SaveChanges();
			}
		}
	}

	public class User
	{
		public string Id { get; set; }
		public string Name { get; set; }
	}
}
