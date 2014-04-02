using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Tests.Bugs.TransformResults;
using Raven.Tests.Common;

using Xunit;

namespace Raven.SlowTests.Bugs
{
	public class TimeoutTester : RavenTest
	{
		[Fact]
		public void will_timeout_query_after_some_time()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new Answers_ByAnswerEntity().Execute(store);
				var answerId = "";

				store.Conventions.MaxNumberOfRequestsPerSession = 1000000; // 1 Million
				CreateEntities(store, 0);

				const string content = "This is doable";

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					AnswerEntity answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
						.Statistics(out stats)
						.Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
						.OrderBy(x => x.Content)
						.Where(x => x.Content == (content))
						.Skip(0).Take(1)
						.As<AnswerEntity>()
						.FirstOrDefault();

					Assert.NotNull(answerInfo);
					answerId = answerInfo.Id;
				}
				List<Task> tasks = new List<Task>();
				object locker = new object();
				for (int k = 0; k < 100; k++)
				{
					var thread = Task.Factory.StartNew(() =>
					{
						lock (locker)
						{
							using (var session = store.OpenSession())
							{
								for (int i = 0; i < 100; i++)
								{
									var answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
										.OrderBy(x => x.Content)
										.Skip(0).Take(1)
										.As<AnswerEntity>()
										.FirstOrDefault();

									Assert.NotNull(answerInfo);
								}
							}
							using (var session = store.OpenSession())
							{
								var answer = session.Load<Answer>(answerId);
								Assert.NotNull(answer);

								answer.Content += k.ToString();
								session.Store(answer);
								session.SaveChanges();
							}
						}
					},TaskCreationOptions.LongRunning);
					tasks.Add(thread);
				}

				Task.WaitAll(tasks.ToArray());
			}
		}

		public static string CreateEntities(IDocumentStore documentStore, int index)
		{
			string questionId = @"question/259" + index;
			string answerId = @"answer/540" + index;
			using (IDocumentSession session = documentStore.OpenSession())
			{
				var user = new User { Id = @"user/222" + index, DisplayName = "John Doe" + index };
				session.Store(user);

				var question = new Question
				{
					Id = questionId,
					Title = "How to do this in RavenDb?" + index,
					Content = "I'm trying to find how to model documents for better DDD support." + index,
					UserId = @"user/222" + index
				};
				session.Store(question);

				var answer = new AnswerEntity
				{
					Id = answerId,
					Question = question,
					Content = "This is doable",
					UserId = user.Id
				};

				session.Store(new Answer
				{
					Id = answer.Id,
					UserId = answer.UserId,
					QuestionId = answer.Question.Id,
					Content = answer.Content
				});

				var vote1 = new AnswerVoteEntity { Id = "votes/1" + index, Answer = answer, QuestionId = questionId, Delta = 2 };
				session.Store(new AnswerVote
				{
					QuestionId = vote1.QuestionId,
					AnswerId = vote1.Answer.Id,
					Delta = vote1.Delta
				});

				var vote2 = new AnswerVoteEntity { Id = "votes/2" + index, Answer = answer, QuestionId = questionId, Delta = 3 };
				session.Store(new AnswerVote
				{
					QuestionId = vote2.QuestionId,
					AnswerId = vote2.Answer.Id,
					Delta = vote2.Delta
				});

				session.SaveChanges();
			}
			return answerId;
		}


	}
}
