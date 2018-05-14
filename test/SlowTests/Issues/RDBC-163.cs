using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBC_163 : RavenTestBase
    {
        public class User
        {
            public User(string personId, List<string> roles, string email, byte[] password, bool isActive, DateTimeOffset? lockedOutUntil = null, Guid? activationGuid = null, Guid? passwordResetGuid = null, string id = null)
            {
                this.PersonId = personId;
                this.Roles = roles;
                this.Email = email;
                this.Password = password;
                this.IsActive = isActive;
                this.LockedOutUntil = lockedOutUntil;
                this.ActivationGuid = activationGuid;
                this.PasswordResetGuid = passwordResetGuid;
            }

            public string Id { get; }

            public string PersonId { get; }

            public List<string> Roles { get; }

            public string Email { get; }

            public byte[] Password { get; set; }

            public bool IsActive { get; }

            public DateTimeOffset? LockedOutUntil { get; }

            public Guid? ActivationGuid { get; }

            public Guid? PasswordResetGuid { get; }
        }


        [Fact]
        public async Task AddUserTest()
        {
            var user = new User("foo", new List<string>(), "foo@bar.com", new byte[] { }, false);
            using(var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user, default);
                    await session.SaveChangesAsync(default);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user2 = await session.LoadAsync<User>(user.Id, default);
                    Assert.NotNull(user2);
                }
            }
        }
    }
}
