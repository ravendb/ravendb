// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4983.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4983 : RavenTestBase
    {

        [Fact]
        public void ShouldLoadUserWithIncludesFromSessionTwiceInShardingDocumentStore()
        {
            TestWithShardedStore(AssertLoadUsersWithIncludes);
        }

        [Fact]
        public void ShouldLoadUserWithIncludesFromSessionTwiceInEmbdeddedDocumentStore()
        {
            using (var store = NewDocumentStore())
            {
                AssertLoadUsersWithIncludes(store);
            }
        }

        [Fact]
        public void ShouldLoadUserFromSessionTwiceInShardingDocumentStore()
        {
            TestWithShardedStore(AssertLoadUsers);
        }

        [Fact]
        public void ShouldLoadUserFromSessionTwiceInEmbdeddedDocumentStore()
        {
            using (var store = NewDocumentStore())
            {
                AssertLoadUsers(store);
            }
        }

        [Fact]
        public void ShouldLazyLoadUserFromSessionTwiceInShardingDocumentStore()
        {
            TestWithShardedStore(AssertLazyLoadUsers);
        }

        [Fact]
        public void ShouldLazyLoadUserFromSessionTwiceInEmbdeddedDocumentStore()
        {
            using (var store = NewDocumentStore())
            {
                AssertLazyLoadUsers(store);
            }
        }

        [Fact]
        public void ShouldMultiLoadUserFromSessionTwiceInShardingDocumentStore()
        {
            TestWithShardedStore(AssertMultiLoadUsers);
        }

        [Fact]
        public void ShouldMultiLoadUserFromSessionTwiceInEmbdeddedDocumentStore()
        {
            using (var store = NewDocumentStore())
            {
                AssertMultiLoadUsers(store);
            }
        }

        private void PrepareDatabase(IDocumentStore documentStore, out User user, out string key)
        {
            user = new User
            {
                Name = "Filipp"
            };

            var userId = user.Id;

            key = documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier(userId, typeof(User), false);

            using (var session = documentStore.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }
        }

        private void AssertLoadUsersWithIncludes(IDocumentStore documentStore)
        {
            var userRoles = new[]
           {
                new UserRole {Name = "Administrator"},
                new UserRole {Name = "Staff"}
            };
            var userRoleIds = userRoles.Select(q => q.Id).ToList();
            var user = new User
            {
                Name = "Filipp",
                UserRoles = userRoleIds
            };

            var userId = user.Id;
            var type = typeof(UserRole);
            var keys =
                userRoleIds.Select(
                    q => documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier(q, type, false));

            using (var session = documentStore.OpenSession())
            {
                session.Store(user);
                foreach (var userRole in userRoles)
                {
                    session.Store(userRole);
                }
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var userFromDb = session.Include<User, UserRole>(x => x.UserRoles).Load(userId);

                Assert.NotNull(userFromDb);

                foreach (var key in keys)
                {
                    Assert.True(session.Advanced.IsLoaded(key));
                }

                var userRolesFromDb = session.Load<UserRole>(userFromDb.UserRoles.Cast<ValueType>());

                foreach (var userRole in userRolesFromDb)
                {
                    Assert.NotNull(userRole);
                    Assert.Equal(userRole.Name, userRoles.First(q => q.Id == userRole.Id).Name);
                }
            }
        }

        private void AssertLoadUsers(IDocumentStore documentStore)
        {
            User user;
            string key;
            PrepareDatabase(documentStore, out user, out key);

            var userId = user.Id;

            using (var session = documentStore.OpenSession())
            {
                var userA = session.Load<User>(userId);
                var userB = session.Load<User>(userId);

                Assert.True(session.Advanced.IsLoaded(key));
                Assert.NotNull(userA);
                Assert.Equal(userA.Name, user.Name);
                Assert.Same(userA, userB);
            }
        }

        private void AssertLazyLoadUsers(IDocumentStore documentStore)
        {
            User user;
            string key;
            PrepareDatabase(documentStore, out user, out key);

            var userId = user.Id;

            using (var session = documentStore.OpenSession())
            {
                var lazyUserA = session.Advanced.Lazily.Load<User>(userId);
                var lazyUserB = session.Advanced.Lazily.Load<User>(userId);

                session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                var userA = lazyUserA.Value;
                var userB = lazyUserB.Value;

                Assert.True(session.Advanced.IsLoaded(key));
                Assert.NotNull(userA);
                Assert.Equal(userA.Name, user.Name);
                Assert.Same(userA, userB);

                var lazyUserC = session.Advanced.Lazily.Load<User>(userId);
                var userC = lazyUserC.Value;

                Assert.NotNull(userC);
            }
        }

        private void AssertMultiLoadUsers(IDocumentStore documentStore)
        {
            User user;
            string key;
            PrepareDatabase(documentStore, out user, out key);

            var userId = user.Id;

            using (var session = documentStore.OpenSession())
            {
                var lazyUsersA = session.Advanced.Lazily.Load<User>(userId, userId);
                var lazyUsersB = session.Advanced.Lazily.Load<User>(userId, userId, userId);

                session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                var usersA = lazyUsersA.Value;
                var usersB = lazyUsersB.Value;

                Assert.Equal(2, usersA.Length);
                Assert.Equal(3, usersB.Length);

                foreach (var user1 in usersA)
                {
                    Assert.NotNull(user1);
                    Assert.Equal(userId, user1.Id);
                }

                foreach (var user1 in usersB)
                {
                    Assert.NotNull(user1);
                    Assert.Equal(userId, user1.Id);
                }
            }
        }

        private void TestWithShardedStore(Action<ShardedDocumentStore> action)
        {
            var server1 = GetNewServer(8079);
            var server2 = GetNewServer(8078);

            var shards = new List<IDocumentStore>
            {
                new DocumentStore {Identifier = "ShardA", Url = server1.Configuration.ServerUrl},
                new DocumentStore {Identifier = "ShardB", Url = server2.Configuration.ServerUrl}
            }.ToDictionary(x => x.Identifier, x => x);

            using (var documentStore = new ShardedDocumentStore(new ShardStrategy(shards)
            {
                ModifyDocumentId = (convention, shardId, documentId) => documentId
            }))
            {
                documentStore.Initialize();

                action(documentStore);
            };
        }

        public class IdObject
        {
            public IdObject()
            {
                Id = Guid.NewGuid();
            }

            public Guid Id { get; set; }
        }

        public class User : IdObject
        {
            public string Name { get; set; }
            public List<Guid> UserRoles { get; set; }
        }

        public class UserRole : IdObject
        {
            public string Name { get; set; }
        }
    }

    
}