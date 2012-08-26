using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.MEF;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Impl;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Xunit;

namespace Raven.Tests.Bugs.TransformResults
{
	public class ComplexValuesFromTransformResults : RavenTest
	{
		[Fact]
		public void CanCreateQueriesWithNestedSelected()
		{
			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				new Answers_ByAnswerEntity2().Execute(documentStore);
			}
		}

		[Fact]
		public void CanExecuteTheTransformResultFunction()
		{
			var answersIndex = new Answers_ByAnswerEntity2 { Conventions = new DocumentConvention() };
			IndexDefinition indexDefinition = answersIndex.CreateIndexDefinition();
			var dynamicViewCompiler = new DynamicViewCompiler("test", indexDefinition, ".");
			AbstractViewGenerator abstractViewGenerator = dynamicViewCompiler.GenerateInstance();
			using (var documentStore = NewDocumentStore())
			{

				Guid questionId = Guid.NewGuid();
				Guid answerId = Guid.NewGuid();

				using (IDocumentSession session = documentStore.OpenSession())
				{
					var user = new User { Id = @"user/222", DisplayName = "John Doe" };
					session.Store(user);

					var question = new Question2
					{
						Id = questionId,
						Title = "How to do this in RavenDb?",
						Content = "I'm trying to find how to model documents for better DDD support.",
						UserId = @"user/222"
					};
					session.Store(question);


					var answer = new AnswerEntity2
					{
						Id = answerId,
						Question = question,
						Content = "This is doable",
						UserId = user.Id
					};
					answer.Votes = new[]
					{
						new AnswerVoteEntity2
						{
							Id = Guid.NewGuid(),
							QuestionId = questionId,
							Answer = answer,
							Delta = 2
						}
					};


					session.Store(new Answer2
					{
						Id = answerId,
						UserId = user.Id,
						QuestionId = question.Id,
						Content = "This is doable",
						Votes = new[]
						{
							new AnswerVote2
							{
								Id = Guid.NewGuid(),
								QuestionId = questionId,
								AnswerId = answerId,
								Delta = 2
							}
						}
					});

					session.SaveChanges();
				}

				documentStore.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				{
					var documentRetriever = new DocumentRetriever(accessor, new OrderedPartCollection<AbstractReadTrigger>());
					var dynamicJsonObjects = new[] { new DynamicJsonObject(accessor.Documents.DocumentByKey("answer2s/" + answerId.ToString(), null).ToJson()), };
					var transformResultsDefinition = abstractViewGenerator.TransformResultsDefinition(documentRetriever,
																									  dynamicJsonObjects
						);

					transformResultsDefinition.ToArray();
				});



			}
		}

		[Fact]
		public void DecimalValues()
		{
			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				new Answers_ByQuestion().Execute(documentStore);

				const string questionId = @"question/259";
				using (IDocumentSession session = documentStore.OpenSession())
				{
					var vote1 = new AnswerVote { QuestionId = questionId, Delta = 2, DecimalValue = 20 };
					session.Store(vote1);
					var vote2 = new AnswerVote { QuestionId = questionId, Delta = 3, DecimalValue = 43 };
					session.Store(vote2);

					session.SaveChanges();
				}

				using (IDocumentSession session = documentStore.OpenSession())
				{
					AnswerViewItem questionInfo = session.Query<AnswerViewItem, Answers_ByQuestion>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.QuestionId == questionId)
						.SingleOrDefault();
					Assert.NotNull(questionInfo);
					Assert.Equal(63, questionInfo.DecimalTotal);
				}
			}
		}

		[Fact]
		public void write_then_read_from_stack_over_flow_types()
		{
			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				new QuestionWithVoteTotalIndex().Execute(documentStore);

				const string questionId = @"question/259";
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

					var vote1 = new QuestionVote { QuestionId = questionId, Delta = 2 };
					session.Store(vote1);
					var vote2 = new QuestionVote { QuestionId = questionId, Delta = 3 };
					session.Store(vote2);

					session.SaveChanges();
				}

				using (IDocumentSession session = documentStore.OpenSession())
				{
					QuestionView questionInfo = session.Query<QuestionView, QuestionWithVoteTotalIndex>()
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
				new Answers_ByAnswerEntity().Execute(documentStore);

				const string questionId = @"question/259";
				string answerId = CreateEntities(documentStore);

				using (IDocumentSession session = documentStore.OpenSession())
				{
					AnswerEntity answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
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
				new Answers_ByAnswerEntity().Execute(documentStore);
				new Votes_ByAnswerEntity().Execute(documentStore);

				string answerId = CreateEntities(documentStore);
				// Working
				using (IDocumentSession session = documentStore.OpenSession())
				{
					AnswerEntity answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Id == answerId)
						.As<AnswerEntity>()
						.SingleOrDefault();
					Assert.NotNull(answerInfo);
					Assert.NotNull(answerInfo.Question);
				}
				// Failing 
				using (IDocumentSession session = documentStore.OpenSession())
				{
					AnswerVoteEntity[] votes = session.Query<AnswerVote, Votes_ByAnswerEntity>()
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
				documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (id, type, allowNull) => id.ToString();

				new Answers_ByAnswerEntity2().Execute(documentStore);
				Guid questionId = Guid.NewGuid();
				Guid answerId = Guid.NewGuid();

				using (IDocumentSession session = documentStore.OpenSession())
				{
					var user = new User { Id = @"user/222", DisplayName = "John Doe" };
					session.Store(user);

					var question = new Question2
					{
						Id = questionId,
						Title = "How to do this in RavenDb?",
						Content = "I'm trying to find how to model documents for better DDD support.",
						UserId = @"user/222"
					};
					session.Store(question);


					var answer = new AnswerEntity2
					{
						Id = answerId,
						Question = question,
						Content = "This is doable",
						UserId = user.Id
					};

					//session.Store(new Answer2
					//{
					//    Id = answer.Id,
					//    UserId = answer.UserId,
					//    QuestionId = answer.Question.Id,
					//    Content = answer.Content
					//}
					session.Store(new Answer2
					{
						Id = answerId,
						UserId = user.Id,
						QuestionId = question.Id,
						Content = "This is doable"
					});

					session.SaveChanges();
				}
				using (IDocumentSession session = documentStore.OpenSession())
				{
					AnswerEntity2 answerInfo = session.Query<Answer2, Answers_ByAnswerEntity2>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Id == answerId)
						.As<AnswerEntity2>()
						.SingleOrDefault();
					Assert.NotNull(answerInfo);
					Assert.NotNull(answerInfo.Question);
				}
			}
		}

		[Fact]
		public void write_then_read_answer_with_votes()
		{
			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (id, type, allowNull) => id.ToString();

				new Answers_ByAnswerEntity2().Execute(documentStore);
				Guid questionId = Guid.NewGuid();
				Guid answerId = Guid.NewGuid();

				using (IDocumentSession session = documentStore.OpenSession())
				{
					var user = new User { Id = @"user/222", DisplayName = "John Doe" };
					session.Store(user);

					var question = new Question2
					{
						Id = questionId,
						Title = "How to do this in RavenDb?",
						Content = "I'm trying to find how to model documents for better DDD support.",
						UserId = @"user/222"
					};
					session.Store(question);


					var answer = new AnswerEntity2
					{
						Id = answerId,
						Question = question,
						Content = "This is doable",
						UserId = user.Id
					};
					answer.Votes = new[]
					{
						new AnswerVoteEntity2
						{
							Id = Guid.NewGuid(),
							QuestionId = questionId,
							Answer = answer,
							Delta = 2
						}
					};


					session.Store(new Answer2
					{
						Id = answerId,
						UserId = user.Id,
						QuestionId = question.Id,
						Content = "This is doable",
						Votes = new[]
						{
							new AnswerVote2
							{
								Id = Guid.NewGuid(),
								QuestionId = questionId,
								AnswerId = answerId,
								Delta = 2
							}
						}
					});

					session.SaveChanges();
				}
				using (IDocumentSession session = documentStore.OpenSession())
				{
					AnswerEntity2 answerInfo = session.Query<Answer2, Answers_ByAnswerEntity2>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Id == answerId)
						.As<AnswerEntity2>()
						.SingleOrDefault();
					Assert.NotNull(answerInfo);
					Assert.NotNull(answerInfo.Votes);
					Assert.True(answerInfo.Votes.Length == 1);
					Assert.True(answerInfo.Votes[0].QuestionId == questionId);
					Assert.NotNull(answerInfo.Votes[0].Answer);
					Assert.True(answerInfo.Votes[0].Answer.Id == answerId);
				}
			}
		}

		[Fact]
		public void will_work_normally_when_querying_multip_times()
		{
			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				new Answers_ByAnswerEntity().Execute(documentStore);

				const string Content = "This is doable";
				const string UserId = @"user/222";

				const string answerId = @"answer/540";
				using (IDocumentSession session = documentStore.OpenSession())
				{
					var user = new User { Id = @"user/222", DisplayName = "John Doe" };
					session.Store(user);


					var answer = new AnswerEntity
									 {
										 Id = answerId,
										 Question = null,
										 Content = "This is doable",
										 UserId = user.Id
									 };

					session.Store(new Answer
									  {
										  Id = answer.Id,
										  UserId = answer.UserId,
										  QuestionId = "",
										  Content = answer.Content
									  });

					session.SaveChanges();
				}
				// Working
				using (IDocumentSession session = documentStore.OpenSession())
				{
					AnswerEntity answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.UserId == UserId && x.Content == Content)
						.As<AnswerEntity>()
						.SingleOrDefault();
					Assert.NotNull(answerInfo);

					AnswerEntity answerInfo2 = session.Query<Answer, Answers_ByAnswerEntity>()
													.Customize(x => x.WaitForNonStaleResultsAsOfNow())
													.Where(x => x.UserId == UserId && x.Content == Content)
													.As<AnswerEntity>()
													.SingleOrDefault();
					Assert.NotNull(answerInfo2);

				}
				// Failing 
				using (IDocumentSession session = documentStore.OpenSession())
				{
					AnswerEntity answerInfo = session.Query<Answer, Answers_ByAnswerEntity>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.UserId == UserId && x.Content == Content)
						.As<AnswerEntity>()
						.SingleOrDefault();
					Assert.NotNull(answerInfo);
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
