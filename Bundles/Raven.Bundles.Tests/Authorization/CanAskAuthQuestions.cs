extern alias client;
using Raven.Bundles.Tests.Versioning;
using client::Raven.Client.Authorization;
using client::Raven.Bundles.Authorization.Model;

using System.Collections.Generic;
using Xunit;

namespace Raven.Bundles.Tests.Authorization
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
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = UserId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Permissions =
						{
							new DocumentPermission
							{
								Role = "Admins",
								Allow = true,
								Operation = "Company/Bid"
							}
						}
				});

				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				var isOperationAllowedOnDocument = s.Advanced.IsOperationAllowedOnDocument(UserId, "Company/Bid", "companies/1");
				Assert.False(isOperationAllowedOnDocument.IsAllowed);
				Assert.Equal("Could not find any permissions for operation: Company/Bid on companies/1 for user Authorization/Users/Ayende.\r\nOnly the following may perform operation Company/Bid on companies/1:\r\n\tOperation: Company/Bid, User: , Role: Admins, Allow: True, Priority: 0\r\n",
					isOperationAllowedOnDocument.Reasons[0]);
			}

			using (var s = store.OpenSession())
			{
				var user = s.Load<AuthorizationUser>(UserId);
				user.Roles = new List<string> { "Admins" };
				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				var isOperationAllowedOnDocument = s.Advanced.IsOperationAllowedOnDocument(UserId, "Company/Bid", "companies/1");
				Assert.True(isOperationAllowedOnDocument.IsAllowed);
				Assert.Equal(new[] { "Operation: Company/Bid, User: , Role: Admins, Allow: True, Priority: 0" }, isOperationAllowedOnDocument.Reasons.ToArray());
			}
		}
	}
}