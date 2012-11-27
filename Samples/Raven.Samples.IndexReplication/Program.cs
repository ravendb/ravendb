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
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Samples.IndexReplication
{
	public class Questions_TitleAndVoteCount : AbstractIndexCreationTask<Question>
	{
		public Questions_TitleAndVoteCount()
		{
			Map = questions => from question in questions
							   select new
							   {
								   question.Title,
								   UpVotes = question.Votes.Count(x => x.Up),
								   DownVotes = question.Votes.Count(x => !x.Up)
							   };
		}
	}

	class Program
	{
		static void Main(string[] args)
		{
			CreateRdbmsSchema();

			using (var documentStore = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				new Questions_TitleAndVoteCount().Execute(documentStore);

				using (var s = documentStore.OpenSession())
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

					var indexReplicationDestination = new Raven.Bundles.IndexReplication.Data.IndexReplicationDestination
					{
						Id = "Raven/IndexReplication/Questions/TitleAndVoteCount",
						ColumnsMapping =
						{
							{"Title", "Title"},
							{"UpVotes", "UpVotes"},
							{"DownVotes", "DownVotes"},
						},
						ConnectionStringName = "Reports",
						PrimaryKeyColumnName = "Id",
						TableName = "QuestionSummaries"
					};

					s.Store(q);
					s.Store(indexReplicationDestination);
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
	[UpVotes] [int] NOT NULL,
	[DownVotes] [int] NOT NULL,
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
