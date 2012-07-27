//-----------------------------------------------------------------------
// <copyright file="ReplicateToRedis.cs" company="Hibernating Rhinos LTD">
//	 Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias database;
using System;
using System.ComponentModel.Composition.Hosting;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Expiration;
using Raven.Bundles.IndexReplicationToRedis;
using Raven.Bundles.IndexReplicationToRedis.Data;
using Raven.Client.Document;
using Raven.Server;
using Xunit;
using ServiceStack.Redis;
using System.Collections.Generic;

namespace Raven.Bundles.Tests.IndexReplicationToRedis
{
	public class ReplicateToRedis : IDisposable
	{
		private const string RedisServerUri = "localhost";

		private readonly DocumentStore documentStore;
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;

		public ConnectionStringSettings ConnectionString { get; set; }

		public ReplicateToRedis()
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
									new AssemblyCatalog(typeof (IndexReplicationToRedisIndexUpdateTrigger).Assembly)
								}
					},
				});
			ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow;
			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
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
		q.QuestionDate,
		SumPoints = q.Votes.Sum(((Func<dynamic,double>)(x=>x.Points)))
	}
",
					Stores =
						{
							{"Title", FieldStorage.Yes},
							{"UpVotes", FieldStorage.Yes},
							{"DownVotes", FieldStorage.Yes},
							{"QuestionDate", FieldStorage.Yes},
							{"SumPoints", FieldStorage.Yes}
						},
					Indexes = { { "Title", FieldIndexing.NotAnalyzed } }
				});
		}

		public void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
		}

		[FactIfRedisDefaultIsAvailable]
		public void Can_replicate_to_redis_using_pocoTypeMode()
		{
			CleanOldRedisData();

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.DatabaseCommands.Delete("Raven/IndexReplicationToRedis/Questions/Votes", null);

				session.Store(new IndexReplicationToRedisDestination
				{
					Id = "Raven/IndexReplicationToRedis/Questions/Votes"
					 ,
					Server = RedisServerUri
					 ,
					RedisSaveMode = IndexReplicationToRedisMode.PocoType,
					PocoTypeAssemblyQualifiedName = typeof(QuestionSummary).AssemblyQualifiedName
				});

				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Title = "How to replicate to Redis?",
					QuestionDate = DateTime.Now.Date,
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!", Points = 1.2}, 
						new Vote{ Up = false, Comment = "Nah!" , Points = 1.3}, 
						new Vote{ Up = true, Comment = "Nice..." , Points = 1.4}
					}
				};
				session.Store(q);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes", "QuestionDate", "SumPoints")
					.ToList();
			}

			using (var redisClient = new RedisClient(RedisServerUri))
			{
				var questions = redisClient.GetAll<QuestionSummary>();

				Assert.True(questions.Count > 0);

				var firstQuestion = questions.First();

				Assert.Equal("questions/1", firstQuestion.Id);
				Assert.Equal("How to replicate to Redis?", firstQuestion.Title);
				Assert.Equal(2, firstQuestion.UpVotes);
				Assert.Equal(1, firstQuestion.DownVotes);
				Assert.Equal(DateTime.Now.Date, firstQuestion.QuestionDate);
			}
		}

		[FactIfRedisDefaultIsAvailable]
		public void Can_replicate_to_redis_when_document_is_updated_using_pocoTypeMode()
		{
			CleanOldRedisData();

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.DatabaseCommands.Delete("Raven/IndexReplicationToRedis/Questions/Votes", null);

				session.Store(new IndexReplicationToRedisDestination
				{
					Id = "Raven/IndexReplicationToRedis/Questions/Votes"
					 ,
					Server = RedisServerUri
					 ,
					RedisSaveMode = IndexReplicationToRedisMode.PocoType,
					PocoTypeAssemblyQualifiedName = typeof(QuestionSummary).AssemblyQualifiedName
					
				});
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Id = "questions/1",
					Title = "How to replicate to Redis?",
					QuestionDate = DateTime.Now.Date,
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!", Points = 1.2}, 
						new Vote{ Up = false, Comment = "Nah!" , Points = 1.3}, 
						new Vote{ Up = true, Comment = "Nice..." , Points = 1.4}, 
					}
				};
				session.Store(q);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes", "QuestionDate", "SumPoints")
					.ToList();
			}

			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Id = "questions/1",
					Title = "How to replicate to Redis?",
					QuestionDate = DateTime.Now.Date,
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!", Points = 1.2}, 
						new Vote{ Up = false, Comment = "Nah!" , Points = 1.3}, 
						new Vote{ Up = true, Comment = "Nice..." , Points = 1.4}, 
						new Vote{ Up = false, Comment = "No!", Points = 1.5}
					  }
				};
				session.Store(q);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes", "QuestionDate", "SumPoints")
					.ToList();
			}

			using (var redisClient = new RedisClient(RedisServerUri))
			{
				var questions = redisClient.GetAll<QuestionSummary>();

				Assert.True(questions.Count > 0);

				var firstQuestion = questions.First();

				Assert.Equal("questions/1", firstQuestion.Id);
				Assert.Equal("How to replicate to Redis?", firstQuestion.Title);
				Assert.Equal(2, firstQuestion.UpVotes);
				Assert.Equal(2, firstQuestion.DownVotes);
				Assert.Equal(DateTime.Now.Date, firstQuestion.QuestionDate);
			}

		}

		[FactIfRedisDefaultIsAvailable]
		public void Can_replicate_to_redis_using_hashMode()
		{
			CleanOldRedisData();

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.DatabaseCommands.Delete("Raven/IndexReplicationToRedis/Questions/Votes", null);

				session.Store(new IndexReplicationToRedisDestination
				{
					Id = "Raven/IndexReplicationToRedis/Questions/Votes"
					 ,
					Server = RedisServerUri
					 ,
					RedisSaveMode = IndexReplicationToRedisMode.RedisHash,
					 
					FieldsToHashSave = new List<string> { 
						 "Title", "QuestionDate", "UpVotes", "DownVotes"
					}
				});
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Title = "How to replicate to Redis?",
					QuestionDate = DateTime.Now.Date,
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!", Points = 1.2}, 
						new Vote{ Up = false, Comment = "Nah!" , Points = 1.3}, 
						new Vote{ Up = true, Comment = "Nice..." , Points = 1.4}
					}
				};
				session.Store(q);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes", "QuestionDate", "SumPoints")
					.ToList();
			}

			using (var redisClient = new RedisClient(RedisServerUri))
			{
				var redisHash = redisClient.GetAllEntriesFromHash("questions/2");

				Assert.True(redisHash != null && redisHash.Count > 0);

				Assert.Equal("How to replicate to Redis?", redisHash["Title"]);
				Assert.Equal(2, Convert.ToInt32(redisHash["UpVotes"]));
				Assert.Equal(1, Convert.ToInt32(redisHash["DownVotes"]));
				Assert.Equal(DateTime.Now.Date, DateTime.ParseExact(redisHash["QuestionDate"], "u", CultureInfo.InvariantCulture));
			}
		}

		[FactIfRedisDefaultIsAvailable]
		public void Can_replicate_to_redis_when_document_is_updated_using_hashMode()
		{
			CleanOldRedisData();

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.DatabaseCommands.Delete("Raven/IndexReplicationToRedis/Questions/Votes", null);

				session.Store(new IndexReplicationToRedisDestination
				{
					Id = "Raven/IndexReplicationToRedis/Questions/Votes"
					 ,
					Server = RedisServerUri
					 ,
					RedisSaveMode = IndexReplicationToRedisMode.RedisHash,

					FieldsToHashSave = new List<string> { 
						 "Title", "QuestionDate", "UpVotes", "DownVotes"
					}
				});
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Id = "questions/2",
					Title = "How to replicate to Redis?",
					QuestionDate = DateTime.Now.Date,
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!", Points = 1.2}, 
						new Vote{ Up = false, Comment = "Nah!" , Points = 1.3}, 
						new Vote{ Up = true, Comment = "Nice..." , Points = 1.4}
					}
				};
				session.Store(q);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes", "QuestionDate", "SumPoints")
					.ToList();
			}

			using (var session = documentStore.OpenSession())
			{
				var q = new Question
				{
					Id = "questions/2",
					Title = "How to replicate to Redis?",
					QuestionDate = DateTime.Now.Date,
					Votes = new[]
					{
						new Vote{ Up = true, Comment = "Good!", Points = 1.2}, 
						new Vote{ Up = false, Comment = "Nah!" , Points = 1.3}, 
						new Vote{ Up = true, Comment = "Nice..." , Points = 1.4}, 
						new Vote{ Up = false, Comment = "No!", Points = 1.5}
					  }
				};
				session.Store(q);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.LuceneQuery<Question>("Questions/Votes")
					.WaitForNonStaleResults()
					.SelectFields<QuestionSummary>("__document_id", "Title", "UpVotes", "DownVotes", "QuestionDate", "SumPoints")
					.ToList();
			}

			using (var redisClient = new RedisClient(RedisServerUri))
			{
				var redisHash = redisClient.GetAllEntriesFromHash("questions/2");

				Assert.True(redisHash != null && redisHash.Count > 0);

				Assert.Equal("How to replicate to Redis?", redisHash["Title"]);
				Assert.Equal(2, Convert.ToInt32(redisHash["UpVotes"]));
				Assert.Equal(2, Convert.ToInt32(redisHash["DownVotes"]));
				Assert.Equal(DateTime.Now.Date, DateTime.ParseExact(redisHash["QuestionDate"], "u", CultureInfo.InvariantCulture));
			}

		}

		private void CleanOldRedisData()
		{
			using (var redisClient = new RedisClient(RedisServerUri))
			{
				redisClient.FlushDb();
			}
		}

		public class QuestionSummary
		{
			public string Id { get; set; }
			public string Title { get; set; }

			public DateTime QuestionDate { get; set; }
			
			public int UpVotes { get; set; }
			public int DownVotes { get; set; }

			public double SumPoints { get; set; }
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
}