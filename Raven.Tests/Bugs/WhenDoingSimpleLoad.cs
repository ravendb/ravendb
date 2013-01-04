using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class WhenDoingSimpleLoad : RavenTest
	{
		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
		}

		[Fact]
		public void WillMakeJustOneRequest()
		{
			using (var server = GetNewServer())
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately,
				}
			}.Initialize())
			{
				using (var session = documentStore.OpenSession())
				{
					var user = session.Load<User>("users/1");
					Assert.Null(user);
				}

				Assert.Equal(1, server.Server.NumberOfRequests);
			}
		}
	}
}