using System.Linq;
using FastTests;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Linq;
using Xunit;

namespace SlowTests.Tests.Bugs.TransformResults
{
    public class SortHintTester : RavenNewTestBase
    {
        [Fact]
        public void will_fail_with_request_headers_too_long()
        {
            using (var store = GetDocumentStore())
            {
                new Answers_ByAnswerEntity().Execute(store);
                new Answers_ByAnswerEntityTransformer().Execute(store);

                store.Conventions.MaxNumberOfRequestsPerSession = 1000000; // 1 Million
                CreateEntities(store);

                const string content = "This is doable";

                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
                           .Statistics(out stats)
                           .Customize(x => x.WaitForNonStaleResults())
                           .OrderBy(x => x.Content)
                           .Where(x => x.Content == (content))
                           .TransformWith<Answers_ByAnswerEntityTransformer, AnswerEntity>()
                           .Skip(0).Take(1)
                           .SingleOrDefault();

                    for (int i = 0; i < 100; i++)
                    {
                        answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
                            .Statistics(out stats)
                            .Where(x => x.Content == (content))
                            .OrderBy(x => x.Content)
                            .TransformWith<Answers_ByAnswerEntityTransformer, AnswerEntity>()
                            .Skip(0).Take(1)
                           .SingleOrDefault();

                        Assert.NotNull(answerInfo);
                    }
                }
            }
        }

        private static string CreateEntities(IDocumentStore documentStore)
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
