using Raven.Tests.Common;
using Xunit;
using System;
using System.Collections.Generic;
using Xunit.Sdk;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
    public class EmptyValues : UniqueConstraintsTest
    {
        [Fact]
        public void When_Creating_User_With_Empty_Email_Value_Should_Not_Create_Constraint_Document()
        {
            var user = new User { Name = "Test", Email = "", TaskIds = null };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var constraints = session.Advanced.LoadStartingWith<object>("UniqueConstraints/");
                Assert.Equal(0, constraints.Length);
            }
        }

        [Fact]
        public void When_Updating_User_With_Empty_Email_Value_Should_Not_Create_Constraint_Document()
        {
            var user = new User { Name = "Test", Email = "foo@bar.com", TaskIds = null };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var existingUser = session.Load<User>(user.Id);

                existingUser.Email = ""; // Empty values are the troublesome
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var constraints = session.Advanced.LoadStartingWith<object>("UniqueConstraints/");
                Assert.Equal(0, constraints.Length);
            }
        }
    }
}
