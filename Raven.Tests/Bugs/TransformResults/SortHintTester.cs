using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs.TransformResults
{
	public class SortHintTester : RavenTest
	{
		[Fact]
		public void will_fail_with_request_headers_too_long()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				new Answers_ByAnswerEntity().Execute(store);

				store.Conventions.MaxNumberOfRequestsPerSession = 1000000; // 1 Million
				CreateEntities(store);

				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();

				const string Content = "This is doable";

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					AnswerEntity answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
						   .Statistics(out stats)
						   .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
						   .OrderBy(x => x.Content)
						   .Where(x => x.Content == (Content))
						   .Skip(0).Take(1)
						   .As<AnswerEntity>()
						   .SingleOrDefault();

					for (int i = 0; i < 100; i++)
					{
						answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
							.Statistics(out stats)
							.Where(x => x.Content == (Content))
							.OrderBy(x => x.Content)
							.Skip(0).Take(1)
						   .As<AnswerEntity>()
						   .SingleOrDefault();

						Assert.NotNull(answerInfo);
					}
				}

			}
		}

		public static string CreateEntities(IDocumentStore documentStore)
		{
			const string questionId = @"question/259";
			const string answerId = @"answer/540";
			using (IDocumentSession session = documentStore.OpenSession())
			{
				var user = new User { Id = @"user/222", DisplayName = "John Doe" };
				session.Store(user);

				var question = new Question
				{
					Id = questionId,
					Title = "How to do this in RavenDb?",
					Content = "I'm trying to find how to model documents for better DDD support.",
					UserId = @"user/222"
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

				var vote1 = new AnswerVoteEntity { Id = "votes/1", Answer = answer, QuestionId = questionId, Delta = 2 };
				session.Store(new AnswerVote
				{
					QuestionId = vote1.QuestionId,
					AnswerId = vote1.Answer.Id,
					Delta = vote1.Delta
				});

				var vote2 = new AnswerVoteEntity { Id = "votes/2", Answer = answer, QuestionId = questionId, Delta = 3 };
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
