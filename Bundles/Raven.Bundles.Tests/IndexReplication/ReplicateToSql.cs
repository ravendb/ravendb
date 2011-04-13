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
using System.Reflection;
using System.Xml;
using Raven.Bundles.Expiration;
using Raven.Bundles.IndexReplication;
using Raven.Bundles.IndexReplication.Data;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Raven.Server;
using Xunit;
using System.Linq;
using Xunit.Sdk;

namespace Raven.Bundles.Tests.IndexReplication
{
    public class ReplicateToSql : IDisposable
    {
        private readonly DocumentStore documentStore;
        private readonly string path;
        private readonly RavenDbServer ravenDbServer;

    	public ConnectionStringSettings ConnectionString { get; set; }

    	public ReplicateToSql()
        {
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(Versioning.Versioning)).CodeBase);
            path = Path.Combine(path, "TestDb").Substring(6);
            database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
            ravenDbServer = new RavenDbServer(
                new database::Raven.Database.Config.RavenConfiguration
                {
                    Port = 58080,
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
            documentStore = new DocumentStore
            {
                Url = "http://localhost:58080"
            };
            documentStore.Initialize();

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

        #region IDisposable Members

        public void Dispose()
        {
            documentStore.Dispose();
            ravenDbServer.Dispose();
            database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
        }

        #endregion

		[FactIfSqlServerIsAvailable]
        public void Can_replicate_to_sql()
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
        public void Can_replicate_to_sql_when_document_is_updated()
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

        public class QuestionSummary
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public int UpVotes { get; set; }
            public int DownVotes { get; set; }
        }

        public class Question
        {
            public string Id { get; set; }
            public string Title { get; set; }

            public Vote[] Votes { get; set; }
        }

        public class Vote
        {
            public bool Up { get; set; }
            public string Comment { get; set; }
        }

    }

	public class FactIfSqlServerIsAvailable : FactAttribute
	{

		ConnectionStringSettings connectionStringSettings;
		public FactIfSqlServerIsAvailable()
		{
			var connectionStringName = GetAppropriateConnectionStringNameInternal();
			if(connectionStringName == null)
			{
				base.Skip = "Could not find a connection string with a valid database to connect to, skipping the test";
				return;
			}
			connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
		}

		protected override System.Collections.Generic.IEnumerable<Xunit.Sdk.ITestCommand> EnumerateTestCommands(Xunit.Sdk.IMethodInfo method)
		{
			return base.EnumerateTestCommands(method).Select(enumerateTestCommand => new ActionTestCommandWrapper(enumerateTestCommand, o =>
			{
				((ReplicateToSql)o).ConnectionString=connectionStringSettings;
			}));
		}

		public class ActionTestCommandWrapper : ITestCommand
		{
			private readonly ITestCommand inner;
			private readonly Action<object> action;

			public ActionTestCommandWrapper(ITestCommand inner, Action<object> action)
			{
				this.inner = inner;
				this.action = action;
			}

			public MethodResult Execute(object testClass)
			{
				action(testClass);
				return inner.Execute(testClass);
			}

			public XmlNode ToStartXml()
			{
				return inner.ToStartXml();
			}

			public string DisplayName
			{
				get { return inner.DisplayName; }
			}

			public bool ShouldCreateInstance
			{
				get { return inner.ShouldCreateInstance; }
			}

			public int Timeout
			{
				get { return inner.Timeout; }
			}
		}

		private static string GetAppropriateConnectionStringNameInternal()
		{
			foreach (ConnectionStringSettings connectionString in new[]
            {
                ConfigurationManager.ConnectionStrings["SqlExpress"],
                ConfigurationManager.ConnectionStrings["LocalHost"],
            })
			{
				var providerFactory = DbProviderFactories.GetFactory(connectionString.ProviderName);
				try
				{
					using (var connection = providerFactory.CreateConnection())
					{
						connection.ConnectionString = connectionString.ConnectionString;
						connection.Open();
					}
					return connectionString.Name;
				}
				// ReSharper disable EmptyGeneralCatchClause
				catch
				// ReSharper restore EmptyGeneralCatchClause
				{
				}
			}
			return null;
		}
	}

}
