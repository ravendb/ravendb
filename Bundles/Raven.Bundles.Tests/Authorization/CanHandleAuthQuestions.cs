//-----------------------------------------------------------------------
// <copyright file="CanHandleAuthQuestions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using client::Raven.Client.Authorization;
using client::Raven.Bundles.Authorization.Model;
using Raven.Bundles.Authorization;
using Xunit;

namespace Raven.Bundles.Tests.Authorization
{
	public class CanHandleAuthQuestions : AuthorizationTest
	{
		private readonly AuthorizationDecisions authorizationDecisions;
		const string userId = "Authorization/Users/Ayende";
		private const string operation = "Company/Solicit";

		public CanHandleAuthQuestions()
		{
			authorizationDecisions = new AuthorizationDecisions(server.Database);
		}

		[Fact]
		public void WhenGivingPermissionOnDocumentRoleAndAssociatingUserWithRoleWillAllow()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" }
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Permissions =
						{
							new DocumentPermission
							{
								Allow = true,
								Operation = operation,
								Role = "Authorization/Roles/Managers"
							}
						}
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}

		[Fact]
		public void GivingPermissionToRoleOnTagAssociatedWithRoleWillAllow()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" }
				});

				s.Store(new AuthorizationRole
				{
					Id = "Authorization/Roles/Managers",
					Permissions =
						{
							new OperationPermission
							{
								Allow = true,
								Operation = operation,
								Tags = { "Fortune 500" }
							}
						}
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Tags = { "Fortune 500" }
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}

		[Fact]
		public void GivingPermissionToRoleOnMultiTagsAssociatedWithRoleWithMultiTagsOnDocumentWillAllow()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" }
				});

				s.Store(new AuthorizationRole
				{
					Id = "Authorization/Roles/Managers",
					Permissions =
						{
							new OperationPermission
							{
								Allow = true,
								Operation = operation,
								Tags = { "Fortune 500", "Technology" }
							}
						}
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Tags = { "Fortune 500", "Technology/Application Software" }
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}

		[Fact]
		public void GivingPermissionToRoleOnMultiTagsAssociatedWithRoleWithoutMultiTagsOnDocumentWillDeny()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" }
				});

				s.Store(new AuthorizationRole
				{
					Id = "Authorization/Roles/Managers",
					Permissions =
						{
							new OperationPermission
							{
								Allow = true,
								Operation = operation,
								Tags = { "Fortune 500", "Technology" }
							}
						}
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Tags = { "Fortune 500" }
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.False(isAllowed);
		}

		[Fact]
		public void GivingPermissionToRoleOnMultiTagsAssociatedWithRoleWithMoreGeneralTagsOnDocumentWillDeny()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" }
				});

				s.Store(new AuthorizationRole
				{
					Id = "Authorization/Roles/Managers",
					Permissions =
						{
							new OperationPermission
							{
								Allow = true,
								Operation = operation,
								Tags = { "Fortune 500", "Technology/Application Sofware" }
							}
						}
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Tags = { "Fortune 500", "Technology" }
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.False(isAllowed);
		}

		[Fact]
		public void GivingPermissionToRoleOnTagAssociatedWithRoleWillAllow_OnClient()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" }
				});

				s.Store(new AuthorizationRole
				{
					Id = "Authorization/Roles/Managers",
					Permissions =
						{
							new OperationPermission
							{
								Allow = true,
								Operation = operation,
								Tags = { "Fortune 500" }
							}
						}
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Tags = { "Fortune 500" }
				});

				s.SaveChanges();

			}

			using (var s = store.OpenSession())
			{
				var authorizationUser = s.Load<AuthorizationUser>(userId);
				Assert.True(s.IsAllowed(authorizationUser, operation));
			}
		}

		[Fact]
		public void GivingDenyPermissionWillReturnFalse_OnClient()
		{
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" },
					Permissions =
						{
							new OperationPermission
							{
								Allow = false,
								Operation = operation,
								Tags = { "Important" }
							}
						}
				});
				s.SaveChanges();
			}
			
			using (var s = store.OpenSession())
			{
				var authorizationUser = s.Load<AuthorizationUser>(userId);
				Assert.False(s.IsAllowed(authorizationUser, operation));
			}
		}

		[Fact]
		public void GivingPermissionOnRoleWorks_OnClient()
		{
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" },
				   
				});
				s.Store(new AuthorizationRole
				{
					Id = "Authorization/Roles/Managers",
					Permissions =
						{
							new OperationPermission
							{
								Allow = true,
								Operation = operation,
								Tags = { "/Important" }
							}
						}
				});
				s.SaveChanges();
			}

			using (var s = store.OpenSession())
			{
				var authorizationUser = s.Load<AuthorizationUser>(userId);
				Assert.True(s.IsAllowed(authorizationUser, operation));
			}
		}

		[Fact]
		public void GivingPermissionForAllowAndDenyOnSameLevelWithReturnDeny()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers" },
					Permissions =
						{
							new OperationPermission
							{
								Allow = false,
								Operation = operation,
								Tags = { "Important" }
							}
						}
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Tags = { "Important" },
					Permissions =
						{
							new DocumentPermission
							{
								Allow = true,
								Operation = operation,
								Role = "Authorization/Roles/Managers"
							}
						}
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.False(isAllowed);
		}

		[Fact]
		public void WhenGivingPermissionOnDocumentRoleAndAssociatingUserWithChildRoleWillAllow()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Roles = { "Authorization/Roles/Managers/Supreme" }
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Permissions =
						{
							new DocumentPermission
							{
								Allow = true,
								Operation = operation,
								Role = "Authorization/Roles/Managers"
							}
						}
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}

		[Fact]
		public void WhenGivingUserPermissionForTagAndTaggingDocumentWillAllow()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Permissions =
						{
							new OperationPermission
							{
								Allow = true,
								Operation = operation,
								Tags = { "Companies/Important" }
							}
						}
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Tags = { "Companies/Important" }
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}

		[Fact]
		public void WhenGivingUserPermissionForParentTagAndTaggingDocumentWillAllow()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
					Permissions =
						{
							new OperationPermission
							{
								Allow = true,
								Operation = operation,
								Tags = { "Companies" }
							}
						}
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Tags = { "Companies/Important" }
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}

		[Fact]
		public void CanGiveUserExplicitPermissionOnDoc()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization
				{
					Permissions =
						{
							new DocumentPermission
							{
								Allow = true,
								Operation = operation,
								User = userId
							}
						}
				});

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}

		[Fact]
		public void WhenThereIsNoPermissionButThereIsAuthorizationWillDeny()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
				});

				s.Store(company);

				s.SetAuthorizationFor(company, new DocumentAuthorization());

				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.False(isAllowed);
		}

		[Fact]
		public void WhenThereIsNoAuthorizationWillAllow()
		{
			var company = new Company
			{
				Name = "Hibernating Rhinos"
			};
			using (var s = store.OpenSession())
			{
				s.Store(new AuthorizationUser
				{
					Id = userId,
					Name = "Ayende Rahien",
				});

				s.Store(company);
				s.SaveChanges();
			}

			var jsonDocument = server.Database.Get(company.Id, null);
			var isAllowed = authorizationDecisions.IsAllowed(userId, operation, company.Id, jsonDocument.Metadata, null);
			Assert.True(isAllowed);
		}
	}
}
