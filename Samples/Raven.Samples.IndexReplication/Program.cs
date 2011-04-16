//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Samples.IndexReplication
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateRdbmsSchema();

            using (var documentStore = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
            {
                documentStore.DatabaseCommands.PutIndex("Questions/VoteTotals",
                                                        new IndexDefinitionBuilder<Question>
                                                        {
                                                            Map = questions => from question in questions
                                                                               select new
                                                                               {
                                                                                   question.Title,
                                                                                   VoteCount = question.Votes.Count
                                                                               }
                                                        },
                                                        overwrite: true);

                using(var s = documentStore.OpenSession())
                {
                    var q = new Question
                    {
                        Id = "questions/1",
                        Title = "How to replicate to SQL Server!?",
                        Votes = new List<Vote>
                        {
                            new Vote {Up = true, Comment = "Good!"},
                            new Vote {Up = false, Comment = "Nah!"},
                            new Vote {Up = true, Comment = "Nice..."},
                            new Vote {Up = false, Comment = "No!"},
                        }
                    };
                    s.Store(q);
                    s.SaveChanges();
                }

            }
        }

        private static void CreateRdbmsSchema()
        {
            var connectionStringSettings = ConfigurationManager.ConnectionStrings["Reports"];
            var providerFactory = DbProviderFactories.GetFactory(connectionStringSettings.ProviderName);
            using (var con = providerFactory.CreateConnection())
            {
                con.ConnectionString = connectionStringSettings.ConnectionString;
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
	[VoteCount] [int] NOT NULL,
	[Title] [nvarchar](255) NOT NULL
)
";
                    dbCommand.ExecuteNonQuery();
                }
            }
        }
    }


    public class Question
    {
        public string Id { get; set; }
        public string Title { get; set; }

        public List<Vote> Votes { get; set; }
    }

    public class Vote
    {
        public bool Up { get; set; }
        public string Comment { get; set; }
    }
}
