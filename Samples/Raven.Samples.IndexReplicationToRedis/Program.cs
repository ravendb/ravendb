//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Bundles.IndexReplicationToRedis.Data;
using Raven.Samples.IndexReplicationToRedis.PocoTypes;
using ServiceStack.Redis;


namespace Raven.Samples.IndexReplicationToRedis
{
	public class Questions_VoteTotals : AbstractIndexCreationTask<Question>
	{
		public Questions_VoteTotals()
		{
			Map = questions => from question in questions
			                   select new
				                   {
					                   question.Title,
					                   UpVotes = question.Votes.Count(x => x.Up),
					                   DownVotes = question.Votes.Count(x => !x.Up),
					                   question.QuestionDate,
					                   SumPoints = question.Votes.Sum(x => x.Points)
				                   };

			Store("Title", FieldStorage.Yes);
			Store("UpVotes", FieldStorage.Yes);
			Store("DownVotes", FieldStorage.Yes);
			Store("QuestionDate", FieldStorage.Yes);
			Store("SumPoints", FieldStorage.Yes);

			Index("Title", FieldIndexing.NotAnalyzed);
		}
	}

	class Program
	{
		static void Main(string[] args)
		{
			var exampleMode = IndexReplicationToRedisMode.RedisHash;
			var exampleDocumentId = "questions/1";

			var ravenDbServer = "http://localhost:8080";
			var redisServer	  = "localhost";

			Console.WriteLine("IndexReplicationToRedisMode: {0}", exampleMode);

			Console.WriteLine("Connectiong RavenDb {0}...", ravenDbServer);

			using (var documentStore = new DocumentStore { Url = ravenDbServer }.Initialize())
			{
				//1) INDEX CREATION
				var indexTask = new Questions_VoteTotals();
				
				indexTask.Execute(documentStore);

				Console.WriteLine("Questions_VoteTotals index created");
				
				using(var s = documentStore.OpenSession())
				{

					Console.WriteLine("Creating document to save bundle configuration...");

					//2) CREATE THE "RAVEN SYSTEM DOCUMENT" TO SAVE BUNDLE´S CONFIG (DEPENDES ON EXAMPLE MODE)
					if (exampleMode == IndexReplicationToRedisMode.RedisHash)
					{
						//2.1) REDIS HASH MODE
						s.Store(new Bundles.IndexReplicationToRedis.Data.IndexReplicationToRedisDestination
						{
							Id = String.Concat("Raven/IndexReplicationToRedis", "/", indexTask.IndexName), //NAME OF INDEX TO REPLICATE TO REDIS
							Server = "localhost", //REDIS SERVER (YOU CAN ADD THE PORT)
							RedisSaveMode = IndexReplicationToRedisMode.RedisHash, //THIS MODE ALLOW GET ITEM BY ID WITH REDIS CLIENT
							FieldsToHashSave = new List<string> { "Title", "UpVotes", "DownVotes", "QuestionDate", "SumPoints" } //PROPERTIES TO SAVE
						});
					}
					else
					{
						//2.2) REDIS CLIENT POCO TYPE MODE
						s.Store(new Bundles.IndexReplicationToRedis.Data.IndexReplicationToRedisDestination
							{
								Id = String.Concat("Raven/IndexReplicationToRedis", "/", indexTask.IndexName) //NAME OF INDEX TO REPLICATE TO REDIS
								,Server = "localhost" //REDIS SERVER (YOU CAN ADD THE PORT
								,RedisSaveMode = IndexReplicationToRedisMode.PocoType //ALLOW USING POCO TYPES WITH REDIS
								,PocoTypeAssemblyQualifiedName = typeof(QuestionSummary).AssemblyQualifiedName //The AssemblyQualifiedName of the type to map with index
								//This type needs the same properties of your index and one string Id property to save the document key
							});
					}

					Console.WriteLine("Creating example document...");

					//3) CREATE EXAMPLE DOCUMENT
					var q = new Question
					{
						Id = exampleDocumentId,
						Title = "How to replicate to Redis?",
						QuestionDate = DateTime.Now,
						Votes = new[]
						{
							new Vote{ Up = true, Comment = "Good!", Points = 1.2}, 
							new Vote{ Up = false, Comment = "Nah!" , Points = 1.3}, 
							new Vote{ Up = true, Comment = "Nice..." , Points = 1.4}, 
							new Vote{ Up = false, Comment = "No!", Points = 1.5}
						}
					};

					Console.WriteLine("Saving data");
					//4) SAVING DATA ON RAVENDB
					s.Store(q);
					s.SaveChanges();
				}

			}

			Console.WriteLine("Checking data on Redis server on {0}", redisServer);

			//5) FIND DATA ON REDIS SERVER
			using (var redisClient = new RedisClient(redisServer))
			{
				var messageFormat = "The Id {0} has this properties => Title: '{1}', UpVotes: {2}, DownVotes: {3}, QuestionDate: {4:dd/MM/yyyy HH:mm} and SumPoints: {5}\n";
				
				if (exampleMode == IndexReplicationToRedisMode.RedisHash)
				{
					var redisHash = redisClient.GetAllEntriesFromHash(exampleDocumentId);

					Console.Write(messageFormat, 
						exampleDocumentId, 
						redisHash["Title"], 
						redisHash["UpVotes"],
						redisHash["DownVotes"],
						DateTime.ParseExact(redisHash["QuestionDate"], "u", CultureInfo.CurrentCulture),
						redisHash["SumPoints"]);
				}
				else
				{
					var pocoType = redisClient.GetById<QuestionSummary>(exampleDocumentId);

					Console.Write(messageFormat,
						exampleDocumentId,
						pocoType.Title,
						pocoType.UpVotes,
						pocoType.DownVotes,
						pocoType.QuestionDate,
						pocoType.SumPoints);
				}
			}

			Console.ReadLine();
		}

	}

	public class Question
	{
		public string Id { get; set; }
		public string Title { get; set; }
		public DateTime QuestionDate { get; set; }

		public Vote[] Votes { get; set; }
	}

	public class Vote
	{
		public bool Up { get; set; }
		public double Points { get; set; }
		public string Comment { get; set; }
	}
}


