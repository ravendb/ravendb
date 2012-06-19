//-----------------------------------------------------------------------
// <copyright file="ReplicateToSql.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias database;
using System;
using System.ComponentModel.Composition.Hosting;
using System.Configuration;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Expiration;
using Raven.Bundles.IndexReplication;
using Raven.Bundles.IndexReplication.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Server;
using Xunit;

namespace Raven.Bundles.Tests.IndexReplication
{
	public class CanReplicateToSql : IDisposable
	{
		private readonly IDocumentStore documentStore;
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;

		private ConnectionStringSettings ConnectionString
		{
			get { return FactIfSqlServerIsAvailable.ConnectionStringSettings; }
		}

		public CanReplicateToSql()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(Versioning.Versioning)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
			ravenDbServer = new RavenDbServer(
				new database::Raven.Database.Config.RavenConfiguration
				{
					Port = 8079,
					DataDirectory = path,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
					Catalog =
						{
							Catalogs =
								{
									new AssemblyCatalog(typeof (IndexReplicationIndexUpdateTrigger).Assembly)
								}
						},
				});
			ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow;
			documentStore = new DocumentStore {Url = "http://localhost:8079"}.Initialize();

			documentStore.DatabaseCommands.PutIndex(
				"Questions/Votes",
				new IndexDefinition
				{
					Map =
						@"
from q in docs.Questions
select new 
	{
		q.Title, 
		UpVotes = q.Votes.Count(((Func<dynamic,bool>)(x=>x.Up))), 
		DownVotes = q.Votes.Count(((Func<dynamic,bool>)(x=>!x.Up))),
		Date = new System.DateTime(2000,1,1)
	}
",
					Stores =
						{
							{"Title", FieldStorage.Yes},
							{"UpVotes", FieldStorage.Yes},
							{"DownVotes", FieldStorage.Yes}
						},
					Indexes = {{"Title", FieldIndexing.NotAnalyzed}}
				});
		}

		private void CreateRdbmsSchema()
		{
			var providerFactory = DbProviderFactories.GetFactory(ConnectionString.ProviderName);
			using (var con = providerFactory.CreateConnection())
			{
				con.ConnectionString = ConnectionString.ConnectionString;
				con.Open();

				using (var dbCommand = con.CreateCommand())
				{
					dbCommand.CommandText = @"
IF OBJECT_ID('QuestionSummaries') is not null 
	DROP TABLE [dbo].[QuestionSummaries]
";
					dbCommand.ExecuteNonQuery();

					dbCommand.CommandText = @"
CREATE TABLE [dbo].[QuestionSummaries]
(
	[Id] [nvarchar](50) NOT NULL,
	[UpVotes] [int] NOT NULL,
	[DownVotes] [int] NOT NULL,
	[Title] [nvarchar](255) NOT NULL,
	[Date] [datetime] NOT NULL
)
";
					dbCommand.ExecuteNonQuery();
				}
			}
		}

		public void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
		}

		[FactIfSqlServerIsAvailable]
		public void WhenInserted()
		{
			CreateRdbmsSchema();

			using (var session = documentStore.OpenSession())
			{
				session.Store(new IndexReplicationDestination
				{
					Id = "Raven/IndexReplication/Questions/Votes",
					ColumnsMapping =
						{
							{"Title", "Title"},
							{"UpVotes", "UpVotes"},
							{"DownVotes", "DownVotes"},
							{"Date", "Date"}
						},
					ConnectionStringName = ConnectionString.Name,
					PrimaryKeyColumnName = "Id",
					TableName = "QuestionSummaries"
				});
				session.SaveChanges();
			}
			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Title = "How to replicate to SQL Server?",
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!"}, 
						new Vote{ Up = false, Comment = "Nah!"}, 
						new Vote{ Up = true, Comment = "Nice..."}, 
					}
				};
				session.Store(q);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes")
					.ToList();
			}

			var providerFactory = DbProviderFactories.GetFactory(ConnectionString.ProviderName);
			using(var con = providerFactory.CreateConnection())
			{
				con.ConnectionString = ConnectionString.ConnectionString;
				con.Open();

				using(var dbCommand = con.CreateCommand())
				{
					dbCommand.CommandText = "SELECT * FROM QuestionSummaries";
					using(var reader = dbCommand.ExecuteReader())
					{
						Assert.True(reader.Read());

						Assert.Equal("questions/1", reader["Id"]);
						Assert.Equal("How to replicate to SQL Server?", reader["Title"]);
						Assert.Equal(2, reader["UpVotes"]);
						Assert.Equal(1, reader["DownVotes"]);
					}
				}
			}
		}

		[FactIfSqlServerIsAvailable]
		public void WhenUpdated()
		{
			CreateRdbmsSchema();
			
			using (var session = documentStore.OpenSession())
			{
				session.Store(new IndexReplicationDestination
				{
					Id = "Raven/IndexReplication/Questions/Votes",
					ColumnsMapping =
						{
							{"Title", "Title"},
							{"UpVotes", "UpVotes"},
							{"DownVotes", "DownVotes"},
							{"Date", "Date"},
						},
					ConnectionStringName = ConnectionString.Name,
					PrimaryKeyColumnName = "Id",
					TableName = "QuestionSummaries"
				});
				session.SaveChanges();
			}
			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Id = "questions/1",
					Title = "How to replicate to SQL Server?",
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!"}, 
						new Vote{ Up = false, Comment = "Nah!"}, 
						new Vote{ Up = true, Comment = "Nice..."}, 
					}
				};
				session.Store(q);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes")
					.ToList();
			}

			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Id = "questions/1",
					Title = "How to replicate to SQL Server!?",
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!"}, 
						new Vote{ Up = false, Comment = "Nah!"}, 
						new Vote{ Up = true, Comment = "Nice..."}, 
						new Vote{ Up = false, Comment = "No!"}, 
					  }
				};
				session.Store(q, q.Id);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes")
					.ToList();
			}

			var providerFactory = DbProviderFactories.GetFactory(ConnectionString.ProviderName);
			using (var con = providerFactory.CreateConnection())
			{
				con.ConnectionString = ConnectionString.ConnectionString;
				con.Open();

				using (var dbCommand = con.CreateCommand())
				{
					dbCommand.CommandText = "SELECT * FROM QuestionSummaries";
					using (var reader = dbCommand.ExecuteReader())
					{
						Assert.True(reader.Read());

						Assert.Equal("questions/1", reader["Id"]);
						Assert.Equal("How to replicate to SQL Server!?", reader["Title"]);
						Assert.Equal(2, reader["UpVotes"]);
						Assert.Equal(2, reader["DownVotes"]);
						Assert.Equal(new DateTime(2000,1,1), reader["Date"]);
					}
				}
			}
		}

		private class QuestionSummary
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public int UpVotes { get; set; }
			public int DownVotes { get; set; }
		}

		private class Question
		{
			public string Id { get; set; }
			public string Title { get; set; }

			public Vote[] Votes { get; set; }
		}

		private class Vote
		{
			public bool Up { get; set; }
			public string Comment { get; set; }
		}
	}
}