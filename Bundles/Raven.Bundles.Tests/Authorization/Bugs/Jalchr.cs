using System.Collections.Generic;
using Raven.Bundles.Authorization.Model;
using Raven.Client.Authorization;
using Xunit;

namespace Raven.Bundles.Tests.Authorization.Bugs
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
            using (var s = store.OpenSession())
            {
                AuthorizationUser user = new AuthorizationUser { Id = userId, Name = "Name" };
                user.Permissions = new List<OperationPermission>
                {
                    new OperationPermission {Allow = true, Operation = operation}
                };
                s.Store(user);

                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var authorizationUser = s.Load<AuthorizationUser>(userId);
                Assert.True(s.IsAllowed(authorizationUser, operation));
            }
        }
    }
}