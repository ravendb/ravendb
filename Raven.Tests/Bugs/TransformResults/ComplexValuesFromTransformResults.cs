using System;
using System.Linq;
using Raven.Client.Client;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Raven.Tests;
using Xunit;

namespace LiveProjectionsBug
{
    public class ComplexValuesFromTransformResults : LocalClientTest
    {
        //private static EmbeddableDocumentStore GetDocumentStore()
        //{
        //    var documentStore = new EmbeddableDocumentStore() { RunInMemory = true, DataDirectory = @"Data" };
        //    documentStore.Initialize();
        //    return documentStore;
        //}

        [Fact]
        public void write_then_read_from_stack_over_flow_types()
        {
            EmbeddableDocumentStore documentStore = NewDocumentStore();
            IndexCreation.CreateIndexes(typeof(QuestionWithVoteTotalIndex).Assembly, documentStore);

            const string questionId = @"question\259";
            using (var session = documentStore.OpenSession())
            {
                var user = new User { Id = @"user\222", DisplayName = "John Doe" };
                session.Store(user);

                var question = new Question
                                {
                                    Id = questionId,
                                    Title = "How to do this in RavenDb?",
                                    Content = "I'm trying to find how to model documents for better DDD support.",
                                    UserId = @"user\222"
                                };
                session.Store(question);

                var vote1 = new QuestionVote { QuestionId = questionId, Delta = 2 };
                session.Store(vote1);
                var vote2 = new QuestionVote { QuestionId = questionId, Delta = 3 };
                session.Store(vote2);

                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var questionInfo = session.Query<QuestionView, QuestionWithVoteTotalIndex>()
                    .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                    .Where(x => x.QuestionId == questionId)
                    .SingleOrDefault();
                Assert.NotNull(questionInfo);
                Assert.False(string.IsNullOrEmpty(questionInfo.User.DisplayName), "User.DisplayName was not loaded");
            }
        }

        [Fact]
        public void object_id_should_not_be_null_after_loaded_from_transformation()
        {
            EmbeddableDocumentStore documentStore = NewDocumentStore();
            IndexCreation.CreateIndexes(typeof(QuestionWithVoteTotalIndex).Assembly, documentStore);

            const string questionId = @"question\259";
            string answerId = CreateEntities(documentStore);

            using (var session = documentStore.OpenSession())
            {
                var answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
                    .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                    .Where(x => x.Id == answerId)
                    .As<AnswerEntity>()
                    .SingleOrDefault();
                Assert.NotNull(answerInfo);
                Assert.NotNull(answerInfo.Question);
                Assert.NotNull(answerInfo.Question.Id);
                Assert.True(answerInfo.Question.Id == questionId);
            }
        }

        [Fact]
        public void write_then_read_from_complex_entity_types()
        {
            EmbeddableDocumentStore documentStore = NewDocumentStore();
            IndexCreation.CreateIndexes(typeof(QuestionWithVoteTotalIndex).Assembly, documentStore);

            string answerId = CreateEntities(documentStore);
            // Working
            using (var session = documentStore.OpenSession())
            {
                var answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
                    .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                    .Where(x => x.Id == answerId)
                    .As<AnswerEntity>()
                    .SingleOrDefault();
                Assert.NotNull(answerInfo);
                Assert.NotNull(answerInfo.Question);
            }
            // Failing 
            using (var session = documentStore.OpenSession())
            {
                var votes = session.Query<AnswerVote, Votes_ByAnswerEntity>()
                    .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                    .Where(x => x.AnswerId == answerId)
                    .As<AnswerVoteEntity>()
                    .ToArray();
                Assert.NotNull(votes);
                Assert.True(votes.Length == 2);
                Assert.NotNull(votes[0].Answer);
                Assert.True(votes[0].Answer.Id == answerId);
                Assert.NotNull(votes[1].Answer);
                Assert.True(votes[1].Answer.Id == answerId);

            }
        }


        private string CreateEntities(EmbeddableDocumentStore documentStore)
        {
            const string questionId = @"question\259";
            const string answerId = @"answer\540";
            using (var session = documentStore.OpenSession())
            {
                var user = new User { Id = @"user\222", DisplayName = "John Doe" };
                session.Store(user);

                var question = new Question
                                   {
                                       Id = questionId,
                                       Title = "How to do this in RavenDb?",
                                       Content = "I'm trying to find how to model documents for better DDD support.",
                                       UserId = @"user\222"
                                   };
                session.Store(question);

                var answer = new AnswerEntity()
                                 {
                                     Id = answerId,
                                     Question = question,
                                     Content = "This is doable",
                                     UserId = user.Id
                                 };

                Answer answerDb = Map(answer);
                session.Store(answerDb);

                var vote1 = new AnswerVoteEntity { Id = "votes\\1", Answer = answer, QuestionId = questionId, Delta = 2 };
                AnswerVote vote1Db = Map(vote1);
                session.Store(vote1Db);

                var vote2 = new AnswerVoteEntity { Id = "votes\\2", Answer = answer, QuestionId = questionId, Delta = 3 };
                AnswerVote vote2Db = Map(vote2);
                session.Store(vote2Db);

                session.SaveChanges();
            }
            return answerId;
        }

        private Answer Map(AnswerEntity entity)
        {
            var answer = new Answer();
            answer.Id = entity.Id;
            answer.UserId = entity.UserId;
            answer.QuestionId = entity.Question.Id;
            answer.Content = entity.Content;

            return answer;
        }

        private AnswerVote Map(AnswerVoteEntity voteEntity)
        {
            var vote = new AnswerVote();
            vote.QuestionId = voteEntity.QuestionId;
            vote.AnswerId = voteEntity.Answer.Id;
            vote.Delta = voteEntity.Delta;
            return vote;
        }
    }
}