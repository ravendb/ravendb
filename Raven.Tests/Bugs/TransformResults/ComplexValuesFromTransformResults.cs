using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.TransformResults
{
    public class ComplexValuesFromTransformResults : LocalClientTest
    {
		[Fact]
		public void write_then_read_from_stack_over_flow_types()
		{
			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				IndexCreation.CreateIndexes(typeof (QuestionWithVoteTotalIndex).Assembly, documentStore);

				const string questionId = @"question\259";
				using (var session = documentStore.OpenSession())
				{
					var user = new User {Id = @"user\222", DisplayName = "John Doe"};
					session.Store(user);

					var question = new Question
					{
						Id = questionId,
						Title = "How to do this in RavenDb?",
						Content = "I'm trying to find how to model documents for better DDD support.",
						UserId = @"user\222"
					};
					session.Store(question);

					var vote1 = new QuestionVote {QuestionId = questionId, Delta = 2};
					session.Store(vote1);
					var vote2 = new QuestionVote {QuestionId = questionId, Delta = 3};
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
		}

    	[Fact]
        public void object_id_should_not_be_null_after_loaded_from_transformation()
        {
			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				IndexCreation.CreateIndexes(typeof (QuestionWithVoteTotalIndex).Assembly, documentStore);

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
        }

		[Fact]
		public void write_then_read_from_complex_entity_types()
		{
			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				IndexCreation.CreateIndexes(typeof (QuestionWithVoteTotalIndex).Assembly, documentStore);

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
		}

        [Fact]
        public void write_then_read_from_complex_entity_types_with_Guids_as_keys()
        {
            using (EmbeddableDocumentStore documentStore = NewDocumentStore())
            {
				documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (id, type) => id.ToString();

                IndexCreation.CreateIndexes(typeof(QuestionWithVoteTotalIndex).Assembly, documentStore);
                var questionId = Guid.NewGuid();
                var answerId = Guid.NewGuid();

                using (var session = documentStore.OpenSession())
                {
                    var user = new User {Id = @"user\222", DisplayName = "John Doe"};
                    session.Store(user);

                    var question = new Question2
                                       {
                                           Id = questionId,
                                           Title = "How to do this in RavenDb?",
                                           Content = "I'm trying to find how to model documents for better DDD support.",
                                           UserId = @"user\222"
                                       };
                    session.Store(question);


                    session.Store(new Answer2
                    {
                        Id = answerId,
                        UserId = user.Id,
                        QuestionId = question.Id,
                        Content =  "This is doable"
                    });
                    session.SaveChanges();
                }
                using (var session = documentStore.OpenSession())
                {
                    var answerInfo = session.Query<Answer2, Answers_ByAnswerEntity2>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                        .Where(x => x.Id == answerId)
                        .As<AnswerEntity2>()
                        .SingleOrDefault();
                    Assert.NotNull(answerInfo);
                    Assert.NotNull(answerInfo.Question);
                }               
            }
        }

    	public static string CreateEntities(IDocumentStore documentStore)
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

            	session.Store(new Answer
                {
                	Id = answer.Id,
                	UserId = answer.UserId,
                	QuestionId = answer.Question.Id,
                	Content = answer.Content
                });

                var vote1 = new AnswerVoteEntity { Id = "votes\\1", Answer = answer, QuestionId = questionId, Delta = 2 };
            	session.Store(new AnswerVote
                {
                	QuestionId = vote1.QuestionId,
                	AnswerId = vote1.Answer.Id,
                	Delta = vote1.Delta
                });

                var vote2 = new AnswerVoteEntity { Id = "votes\\2", Answer = answer, QuestionId = questionId, Delta = 3 };
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