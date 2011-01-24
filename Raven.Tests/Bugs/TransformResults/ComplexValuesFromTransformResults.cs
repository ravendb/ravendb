using System.Linq;
using Raven.Client.Client;
using Raven.Client.Indexes;
using Raven.Tests;
using Xunit;

namespace LiveProjectionsBug
{
	public class ComplexValuesFromTransformResults : LocalClientTest
	{
		private static EmbeddableDocumentStore GetDocumentStore()
		{
			var documentStore = new EmbeddableDocumentStore() { RunInMemory = true, DataDirectory = @"Data" };
			documentStore.Initialize();
			return documentStore;
		}

		[Fact]
		public void write_then_read_from_stack_over_flow_types()
		{
			EmbeddableDocumentStore documentStore = GetDocumentStore();
			IndexCreation.CreateIndexes(typeof(QuestionWithVoteTotalIndex).Assembly, documentStore);

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
}