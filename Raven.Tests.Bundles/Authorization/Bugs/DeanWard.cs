extern alias client;
using System.Threading;

using Raven.Client;

using Xunit;

using System.Linq;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
	public class DeanWard : AuthorizationTest
	{
		const string Operation = "Content/View";

		public void SecureForCausesHighCpu()
		{

			User user = new User {Name = "Mr. Test"};
			Content contentWithoutPermission = new Content {Title = "Content Without Permission"};
			Content contentWithPermission = new Content {Title = "Content With Permission"};

			using (IDocumentSession session = store.OpenSession())
			{
				session.Store(user);
				session.Store(contentWithoutPermission);
				session.Store(contentWithPermission);

				client::Raven.Bundles.Authorization.Model.DocumentAuthorization authorization = client::Raven.Client.Authorization.AuthorizationClientExtensions.GetAuthorizationFor(session, contentWithoutPermission) ??
				                                      new client::Raven.Bundles.Authorization.Model.DocumentAuthorization();
				authorization.Permissions.Add(new client::Raven.Bundles.Authorization.Model.DocumentPermission {Allow = false, Operation = Operation, User = user.Id});
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session, contentWithoutPermission, authorization);

				authorization = client::Raven.Client.Authorization.AuthorizationClientExtensions.GetAuthorizationFor(session, contentWithPermission) ?? new client::Raven.Bundles.Authorization.Model.DocumentAuthorization();
				authorization.Permissions.Add(new client::Raven.Bundles.Authorization.Model.DocumentPermission {Allow = true, Operation = Operation, User = user.Id});
				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(session, contentWithPermission, authorization);

				session.SaveChanges();
			}

			while (store.DatabaseCommands.GetStatistics().StaleIndexes.Length > 0)
			{
				Thread.Sleep(10);
			}

			for (int i = 0; i < 5; i++)
			{
				using (IDocumentSession session = store.OpenSession())
				{
					client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(session, user.Id, Operation);
					Content contentY = session.Query<Content>().FirstOrDefault();
					Assert.NotNull(contentY);
					Assert.Equal(contentWithPermission.Id, contentY.Id);
				}
			}
		}

		private class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		private class Content
		{
			public string Id { get; set; }
			public string Title { get; set; }
		}
	}
}