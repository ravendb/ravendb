extern alias database;
using System;
using System.ComponentModel.Composition.Hosting;
using System.Configuration;
using System.Data.Common;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Bundles.Expiration;
using Raven.Bundles.ReplicateToSql;
using Raven.Bundles.ReplicateToSql.Data;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Raven.Server;
using Xunit;
using System.Linq;

namespace Raven.Bundles.Tests.ReplicateToSql
{
    public class ReplicateToSql : IDisposable
    {
        private readonly DocumentStore documentStore;
        private readonly string path;
        private readonly RavenDbServer ravenDbServer;

        public ReplicateToSql()
        {
            path = Path.GetDirectoryName(Assembly.GetAssembly(typeof (Versioning)).CodeBase);
            path = Path.Combine(path, "TestDb").Substring(6);
            if (Directory.Exists(path))
                Directory.Delete(path, true);
            ravenDbServer = new RavenDbServer(
                new database::Raven.Database.RavenConfiguration
                {
                    Port = 58080,
                    DataDirectory = path,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                    Catalog =
                        {
                            Catalogs =
                                {
                                    new AssemblyCatalog(typeof (ReplicateToSqlIndexUpdateTrigger).Assembly)
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
        DownVotes = q.Votes.Count(((Func<dynamic,bool>)(x=>!x.Up)))
    }
",
                    Stores =
                        {
                            {"Title", FieldStorage.Yes},
                            {"UpVotes", FieldStorage.Yes},
                            {"DownVotes", FieldStorage.Yes}
                        }
                });
        }

        #region IDisposable Members

        public void Dispose()
        {
            documentStore.Dispose();
            ravenDbServer.Dispose();
            if (Directory.Exists(path))
                Directory.Delete(path, true);
        }

        #endregion

        [Fact]
        public void Can_replicate_to_sql()
        {
            using (var session = documentStore.OpenSession())
            {
                session.Store(new ReplicateToSqlDestination
                {
                    Id = "Raven/ReplicateToSql/Questions/Votes",
                    ColumnsMapping =
                        {
                            {"Title", "Title"},
                            {"UpVotes", "UpVotes"},
                            {"DownVotes", "DownVotes"},
                        },
                    ConnectionStringName = "Reports",
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
                session.LuceneQuery<Question>("Questions/Votes")
                    .WaitForNonStaleResults()
                    .SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes")
                    .ToList();
            }

            var connectionString = ConfigurationManager.ConnectionStrings["Reports"];
            var providerFactory = DbProviderFactories.GetFactory(connectionString.ProviderName);
            using(var con = providerFactory.CreateConnection())
            {
                con.ConnectionString = connectionString.ConnectionString;
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

}