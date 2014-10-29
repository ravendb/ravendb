extern alias client;
using System.Collections.Generic;

using Xunit;

namespace Raven.Tests.Bundles.Authorization
{
	public class CanAskAuthQuestionsFromServer : AuthorizationTest
	{
		[Fact]
		public void CanAskWhateverUserHavePermissions()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession(DatabaseName))
			{
				s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
				{
					Id = UserId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(s, company, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization
				{
					Permissions =
						{
							new client::Raven.Bundles.Authorization.Model.DocumentPermission
							{
								Role = "Admins",
								Allow = true,
								Operation = "Company/Bid"
							}
						}
				});

				s.SaveChanges();
			}

			using (var s = store.OpenSession(DatabaseName))
			{
				var isOperationAllowedOnDocument = client::Raven.Client.Authorization.AuthorizationClientExtensions.IsOperationAllowedOnDocument(s.Advanced, UserId, "Company/Bid", "companies/1");
				Assert.False(isOperationAllowedOnDocument.IsAllowed);
				Assert.Equal("Could not find any permissions for operation: Company/Bid on companies/1 for user Authorization/Users/Ayende.\r\nOnly the following may perform operation Company/Bid on companies/1:\r\n\tOperation: Company/Bid, User: , Role: Admins, Allow: True, Priority: 0\r\n",
					isOperationAllowedOnDocument.Reasons[0]);
			}

			using (var s = store.OpenSession(DatabaseName))
			{
				var user = s.Load<client::Raven.Bundles.Authorization.Model.AuthorizationUser>(UserId);
				user.Roles = new List<string> { "Admins" };
				s.SaveChanges();
			}

			using (var s = store.OpenSession(DatabaseName))
			{
				var isOperationAllowedOnDocument = client::Raven.Client.Authorization.AuthorizationClientExtensions.IsOperationAllowedOnDocument(s.Advanced, UserId, "Company/Bid", "companies/1");
				Assert.True(isOperationAllowedOnDocument.IsAllowed);
				Assert.Equal(new[] { "Operation: Company/Bid, User: , Role: Admins, Allow: True, Priority: 0" }, isOperationAllowedOnDocument.Reasons.ToArray());
			}
		}
	}
}