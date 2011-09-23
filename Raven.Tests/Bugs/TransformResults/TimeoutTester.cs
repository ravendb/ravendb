using System;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Server;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs.TransformResults
{
    public class TimeoutTester : RemoteClientTest
    {
        [Fact]
        public void will_timeout_query_after_some_time()
        {
            using (var server = GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
            {
                new Answers_ByAnswerEntity().Execute(store);

                store.Conventions.MaxNumberOfRequestsPerSession = 1000000; // 1 Million
                CreateEntities(store, 0);

                //WaitForAllRequestsToComplete(server);
                //server.Server.ResetNumberOfRequests();

                const string Content = "This is doable";

                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    AnswerEntity answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
                        .Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                        .OrderBy(x => x.Content)
                        .Where(x => x.Content.Contains(Content))
                        .Skip(0).Take(1)
                        .As<AnswerEntity>()
                        .FirstOrDefault();

                    Assert.NotNull(answerInfo);
                }
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10000; i++)
                    {
                        var answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
                            //.Customize(x => x.WaitForNonStaleResultsAsOfLastWrite(TimeSpan.FromSeconds(15)))
                            //.Statistics(out stats)
                            //.Where(x => x.Content.StartsWith(Content))
                            .OrderBy(x => x.Content)
                            .Skip(0).Take(1)
                            .As<AnswerEntity>()
                            .FirstOrDefault();

                        Console.WriteLine(" i = {0}", i);
                        Assert.NotNull(answerInfo);

                        if (i % 100 == 0)
                        {
                            if (answerInfo != null) // Update it
                            {
                                var answer = session.Load<Answer>(answerInfo.Id);
                                Assert.NotNull(answer);

                                answer.Content += i.ToString();
                                session.Store(answer);
                                session.SaveChanges();
                            }
                        }
                        //CreateEntities(store, i);
                    }
                }

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
