//-----------------------------------------------------------------------
// <copyright file="Jalchr.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias client;
using System.Collections.Generic;

using Xunit;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
	public class Jalchr : AuthorizationTest
	{
		[Fact]
		public void WithStandardUserName()
		{
			var userId = "Users/Ayende";
			ExecuteSecuredOperation(userId);
		}

		[Fact]
		public void WithRavenPrefixUserName()
		{
			var userId = "Raven/Users/Ayende";
			ExecuteSecuredOperation(userId);
		}

		private void ExecuteSecuredOperation(string userId)
		{
			string operation = "operation";
			using (var s = store.OpenSession(DatabaseName))
			{
				client::Raven.Bundles.Authorization.Model.AuthorizationUser user = new client::Raven.Bundles.Authorization.Model.AuthorizationUser { Id = userId, Name = "Name" };
				user.Permissions = new List<client::Raven.Bundles.Authorization.Model.OperationPermission>
				{
					new client::Raven.Bundles.Authorization.Model.OperationPermission {Allow = true, Operation = operation}
				};
				s.Store(user);

				s.SaveChanges();
			}

            using (var s = store.OpenSession(DatabaseName))
			{
				var authorizationUser = s.Load<client::Raven.Bundles.Authorization.Model.AuthorizationUser>(userId);
				Assert.True(client::Raven.Client.Authorization.AuthorizationClientExtensions.IsAllowed(s, authorizationUser, operation));
			}
		}
	}
}