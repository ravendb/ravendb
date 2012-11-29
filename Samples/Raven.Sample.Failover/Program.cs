//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Sample.Failover
{
	class Program
	{
		private const int RetriesCount = 500;

		static void Main(string[] args)
		{
			var documentStore1 = new DocumentStore
				                     {
					                     Url = "http://localhost:8080",
					                     Conventions =
						                     {
							                     FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries,
						                     }
				                     }.Initialize();

			var documentStore2 = new DocumentStore
				                     {
					                     Url = "http://localhost:8081"
				                     }.Initialize();

			CreateDatabase(documentStore1);
			Console.WriteLine("Created Replication database on 8080");
			CreateDatabase(documentStore2);
			Console.WriteLine("Created Replication database on 8081");

			CreateReplication(documentStore1, "http://localhost:8081");
			Console.WriteLine("Created Replication document on 8080");

			var replicationInformerForDatabase = ((DocumentStore)documentStore1).GetReplicationInformerForDatabase("Replication");
			replicationInformerForDatabase.RefreshReplicationInformation((ServerClient)documentStore1.DatabaseCommands.ForDatabase("Replication"));

			using (var session1 = documentStore1.OpenSession("Replication"))
			{
				session1.Store(new User { Id = "users/ayende", Name = "Ayende" });
				session1.SaveChanges();
			}

			WaitForDocument(documentStore2.DatabaseCommands.ForDatabase("Replication"), "users/ayende");

			Console.WriteLine("Wrote one document to 8080, ready for server failure");
			Console.ReadLine();

			using (var session1 = documentStore1.OpenSession("Replication"))
			{
				var name = session1.Load<User>("users/ayende").Name;
				Console.WriteLine(name);
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

			if(commands.Head(expectedId) == null)
				throw new Exception("Document not replicated");
		}

		private static void SetupReplication(IDatabaseCommands source, params string[] urls)
		{
			source.Put(Constants.RavenReplicationDestinations,
			           null, new RavenJObject
				                 {
					                 {
						                 "Destinations", new RavenJArray(urls.Select(url => new RavenJObject
							                                                                    {
								                                                                    {"Url", url}
							                                                                    }))
					                 }
				                 }, new RavenJObject());
		}

		private static void CreateReplication(IDocumentStore documentStore, string url)
		{
			using (var session = documentStore.OpenSession("Replication"))
			{
				var replicationDocument = new ReplicationDocument
				{
					Destinations = new List<ReplicationDestination>
							                                         {
								                                         new ReplicationDestination
									                                         {
										                                         Url = url,
										                                         Database = "Replication"
									                                         }
							                                         }
				};

				session.Store(replicationDocument);
				session.SaveChanges();
			}
		}

		private static void CreateDatabase(IDocumentStore documentStore)
		{
			using (var session = documentStore.OpenSession())
			{
				var databaseDocument = new DatabaseDocument
					                       {
						                       Id = "Raven/Databases/replication",
						                       Settings = new Dictionary<string, string>
							                                  {
								                                  {"Raven/DataDir", "~\\Databases\\replication"},
								                                  {"Raven/ActiveBundles", "Replication"}
							                                  }
					                       };

				session.Store(databaseDocument);
				session.SaveChanges();
			}
		}
	}
}