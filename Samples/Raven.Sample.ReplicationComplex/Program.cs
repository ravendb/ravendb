namespace Raven.Sample.ReplicationComplex
{
	using System;
	using System.Collections.Generic;
	using System.Threading;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Replication;
	using Raven.Client;
	using Raven.Client.Connection;
	using Raven.Client.Document;

	public class Program
	{
		private const string Url1 = "http://localhost:8079";

		private const string Url2 = "http://localhost:8078";

		private const string DatabaseName = "ReplicationComplexSampleDB";

		private const int RetriesCount = 500;

		public static void Main(string[] args)
		{
			var store1 = new DocumentStore
							 {
								 Url = Url1,
								 Conventions =
									 {
										 FailoverBehavior = FailoverBehavior.ReadFromAllServers
									 }
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

			CreateReplication(store1, DatabaseName, Url2, DatabaseName);
			Console.WriteLine("Created Replication document on {0}.", store1.Url);

			var replicationInformerForDatabase = store1.GetReplicationInformerForDatabase(DatabaseName);
			replicationInformerForDatabase.RefreshReplicationInformation((ServerClient)store1.DatabaseCommands.ForDatabase(DatabaseName));

			using (var session = store1.OpenSession(DatabaseName))
			{
				session.Store(new User { Name = "Ayende" }, "users/ayende");
				session.SaveChanges();
			}

			WaitForDocument(store2.DatabaseCommands.ForDatabase(DatabaseName), "users/ayende");

			Console.WriteLine("Document users/ayende successfully replicated to {0}, ready for {1} server failure.", store2.Url, store1.Url);
			Console.WriteLine("Press any key to continue...");
			Console.ReadLine();

			for (int i = 0; i < 12; i++)
			{
				using (var session = store1.OpenSession(DatabaseName))
				{
					session.Load<User>("users/ayende");
				}
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

		private static void WaitForDocument(IDatabaseCommands commands, string expectedId)
		{
			for (int i = 0; i < RetriesCount; i++)
			{
				if (commands.Head(expectedId) != null)
					break;
				Thread.Sleep(100);
			}

			if (commands.Head(expectedId) == null)
				throw new Exception("Document not replicated");
		}
	}

	public class User
	{
		public string Name { get; set; }
	}
}