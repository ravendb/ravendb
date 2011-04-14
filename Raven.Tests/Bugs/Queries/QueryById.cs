using System.Linq;
using LiveProjectionsBug;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class QueryById : LocalClientTest
	{
		[Fact]
		public void CanQueryById()
		{
			using (var store = NewDocumentStore())
			{
				const string questionId = @"question\259";
				using (var session = store.OpenSession())
				{
					var user = new User();
					user.Id = @"user\222";
					user.Name = "John Doe";
					session.Store(user);

					var question = new Question();
					question.Id = questionId;
					question.Title = "How to to this in RavenDb?";
					question.Content = "I'm trying to find how to model documents for better DDD support.";
					question.UserId = @"user\222";
					session.Store(question);

					session.SaveChanges();

					var questionInfo1 = session.Load<Question>(questionId);
					Assert.NotNull(questionInfo1);

					Question questionInfo2 = session.Query<Question>()
						.Customize(y => y.WaitForNonStaleResults())
						.Where(x => x.Id == questionId)
						.SingleOrDefault();
					Assert.NotNull(questionInfo2);

					Question questionInfo3 = session.Query<Question>()
						.Customize(y => y.WaitForNonStaleResults())
						.Where(x => x.UserId == @"user\222")
						.SingleOrDefault();

					Assert.NotNull(questionInfo3);
				}
			}
		}
	}
}