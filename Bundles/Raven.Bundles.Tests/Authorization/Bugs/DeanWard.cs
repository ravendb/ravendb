extern alias client;
using System.Collections.Generic;
using System.Threading;
using Raven.Client;
using Raven.Client.Exceptions;
using client::Raven.Client.Authorization;
using Xunit;
using client::Raven.Bundles.Authorization.Model;
using System.Linq;

namespace Raven.Bundles.Tests.Authorization.Bugs
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

				DocumentAuthorization authorization = session.GetAuthorizationFor(contentWithoutPermission) ??
				                                      new DocumentAuthorization();
				authorization.Permissions.Add(new DocumentPermission {Allow = false, Operation = Operation, User = user.Id});
				session.SetAuthorizationFor(contentWithoutPermission, authorization);

				authorization = session.GetAuthorizationFor(contentWithPermission) ?? new DocumentAuthorization();
				authorization.Permissions.Add(new DocumentPermission {Allow = true, Operation = Operation, User = user.Id});
				session.SetAuthorizationFor(contentWithPermission, authorization);

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
					session.SecureFor(user.Id, Operation);
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