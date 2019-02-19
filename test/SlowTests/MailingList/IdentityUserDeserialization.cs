// -----------------------------------------------------------------------
//  <copyright file="IdentityUserDeserialization.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class IdentityUserDeserialization : RavenTestBase
    {
        [Fact]
        public void Can_Deserialize_IdentityUser()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.CustomizeJsonSerializer += serializer => serializer.ObjectCreationHandling = ObjectCreationHandling.Auto;
                }
            }))
            {
                using (IDocumentSession session = store.OpenSession())
                {
                    var user = new IdentityUser { UserName = "Marcus" };
                    session.Store(user);

                    var l = new IdentityUserLogin<string>
                    {
                        UserId = user.Id,
                        ProviderKey = null,
                        LoginProvider = "Twitter",
                        ProviderDisplayName = "Twitter"
                    };

                    user.Logins.Add(l);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<IdentityUser>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .FirstOrDefault();

                    Assert.Equal("Marcus", result.UserName);
                    Assert.Equal(1, result.Logins.Count());
                }
            }
        }

        private class IdentityUserLogin : IdentityUserLogin<string> { }

        /// <summary>
        ///     Entity type for a user's login (i.e. facebook, google)
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        private class IdentityUserLogin<TKey> where TKey : IEquatable<TKey>
        {
            /// <summary>
            ///     The login provider for the login (i.e. facebook, google)
            /// </summary>
            public virtual string LoginProvider { get; set; }

            /// <summary>
            ///     Key representing the login for the provider
            /// </summary>
            public virtual string ProviderKey { get; set; }

            /// <summary>
            ///     Display name for the login
            /// </summary>
            public virtual string ProviderDisplayName { get; set; }

            /// <summary>
            ///     User Id for the user who owns this login
            /// </summary>
            public virtual TKey UserId { get; set; }
        }

        // from : https://raw.githubusercontent.com/aspnet/Identity/dev/src/Microsoft.AspNet.Identity/IdentityUser.cs
        private class IdentityUser : IdentityUser<string>
        {
            public IdentityUser()
            {
                Id = Guid.NewGuid().ToString();
            }

            public IdentityUser(string userName) : this()
            {
                UserName = userName;
            }
        }

        private class IdentityUser<TKey> where TKey : IEquatable<TKey>
        {
            private ICollection<IdentityUserRole<TKey>> _roles;
            private ICollection<IdentityUserClaim<TKey>> _claims;
            private ICollection<IdentityUserLogin<TKey>> _logins;

            public IdentityUser()
            {
                _roles = new List<IdentityUserRole<TKey>>();
                _claims = new List<IdentityUserClaim<TKey>>();
                _logins = new List<IdentityUserLogin<TKey>>();
            }

            public IdentityUser(string userName) : this()
            {
                UserName = userName;

            }

            public virtual TKey Id { get; set; }
            public virtual string UserName { get; set; }
            public virtual string NormalizedUserName { get; set; }

            /// <summary>
            ///     Email
            /// </summary>
            public virtual string Email { get; set; }

            /// <summary>
            ///     True if the email is confirmed, default is false
            /// </summary>
            public virtual bool EmailConfirmed { get; set; }

            /// <summary>
            ///     The salted/hashed form of the user password
            /// </summary>
            public virtual string PasswordHash { get; set; }

            /// <summary>
            ///     A random value that should change whenever a users credentials change (password changed, login removed)
            /// </summary>
            public virtual string SecurityStamp { get; set; }

            /// <summary>
            ///     PhoneNumber for the user
            /// </summary>
            public virtual string PhoneNumber { get; set; }

            /// <summary>
            ///     True if the phone number is confirmed, default is false
            /// </summary>
            public virtual bool PhoneNumberConfirmed { get; set; }

            /// <summary>
            ///     Is two factor enabled for the user
            /// </summary>
            public virtual bool TwoFactorEnabled { get; set; }

            /// <summary>
            ///     DateTime in UTC when lockout ends, any time in the past is considered not locked out.
            /// </summary>
            public virtual DateTimeOffset LockoutEnd { get; set; }

            /// <summary>
            ///     Is lockout enabled for this user
            /// </summary>
            public virtual bool LockoutEnabled { get; set; }

            /// <summary>
            ///     Used to record failures for the purposes of lockout
            /// </summary>
            public virtual int AccessFailedCount { get; set; }

            /// <summary>
            ///     Roles for the user
            /// </summary>
            public virtual ICollection<IdentityUserRole<TKey>> Roles
            {
                get { return _roles; }
            }

            /// <summary>
            ///     Claims for the user
            /// </summary>
            public virtual ICollection<IdentityUserClaim<TKey>> Claims
            {
                get { return _claims; }
            }

            /// <summary>
            ///     Associated logins for the user
            /// </summary>
            public virtual ICollection<IdentityUserLogin<TKey>> Logins
            {
                get { return _logins; }
            }
        }

        private class IdentityUserRole : IdentityUserRole<string> { }

        /// <summary>
        ///     EntityType that represents a user belonging to a role
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        private class IdentityUserRole<TKey> where TKey : IEquatable<TKey>
        {
            /// <summary>
            ///     UserId for the user that is in the role
            /// </summary>
            public virtual TKey UserId { get; set; }

            /// <summary>
            ///     RoleId for the role
            /// </summary>
            public virtual TKey RoleId { get; set; }
        }

        private class IdentityUserClaim : IdentityUserClaim<string> { }

        /// <summary>
        ///     EntityType that represents one specific user claim
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        public class IdentityUserClaim<TKey> where TKey : IEquatable<TKey>
        {
            /// <summary>
            ///     Primary key
            /// </summary>
            public virtual int Id { get; set; }

            /// <summary>
            ///     User Id for the user who owns this claim
            /// </summary>
            public virtual TKey UserId { get; set; }

            /// <summary>
            ///     Claim type
            /// </summary>
            public virtual string ClaimType { get; set; }

            /// <summary>
            ///     Claim value
            /// </summary>
            public virtual string ClaimValue { get; set; }
        }
    }
}
