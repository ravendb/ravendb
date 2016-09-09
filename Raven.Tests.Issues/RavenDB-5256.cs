// -----------------------------------------------------------------------
//  <copyright file="RavenDB-5256.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_5256 : RavenTest
    {
        [Theory]
        [InlineData("ShardA")]
        [InlineData("ShardB")]
        public void ShouldLoadUserFromSessionTwiceInShardingDocumentStore(string mainShard)
        {
            TestWithShardedStore(AssertLoadUsersWithIncludes, mainShard);
        }

        [Fact]
        public void ShouldLoadUserFromSessionTwiceInEmbeddedDocumentStore()
        {
            using (var store = NewDocumentStore())
            {
                AssertLoadUsersWithIncludes(store, null);
            }
        }

        [Theory]
        [InlineData("ShardA")]
        [InlineData("ShardB")]
        public void ShouldLoadUserFromAsyncSessionTwiceInShardingDocumentStore(string mainShard)
        {
            TestWithShardedStore(AssertLoadUsersWithIncludesAsync, mainShard);
        }

        [Fact]
        public void ShouldLoadUserFromAsyncSessionTwiceInEmbeddedDocumentStore()
        {
            using (var store = NewDocumentStore())
            {
                AssertLoadUsersWithIncludesAsync(store, null);
            }
        }

        [Theory]
        [InlineData("ShardA")]
        [InlineData("ShardB")]
        public void ShouldLoadMultipleUsersWithIncludesFromSessionInShardingDocumentStore(string mainShard)
        {
            TestWithShardedStore(AssertLoadMultipleUsersWithIncludes, mainShard);
        }

        [Fact]
        public void ShouldLoadMultipleUsersWithIncludesFromSessionInEmbeddedDocumentStore()
        {
            using (var store = NewDocumentStore())
            {
                AssertLoadMultipleUsersWithIncludes(store, null);
            }
        }

        [Theory]
        [InlineData("ShardA")]
        [InlineData("ShardB")]
        public void ShouldLoadMultipleUsersWithIncludesFromAsyncSessionInShardingDocumentStore(string mainShard)
        {
            TestWithShardedStore(AssertLoadMultipleUsersWithIncludesAsync, mainShard);
        }

        [Fact]
        public void ShouldLoadMultipleUsersWithIncludesFromAsyncSessionInEmbeddedDocumentStore()
        {
            using (var store = NewDocumentStore())
            {
                AssertLoadMultipleUsersWithIncludesAsync(store, null);
            }
        }

        private void AssertLoadMultipleUsersWithIncludes(IDocumentStore documentStore, string mainShard)
        {
            var userRoles = new[]
            {
                new UserRole {Name = "User Role A", Shard = mainShard},
                new UserRole {Name = "User Role B", Shard = mainShard}
            };
            var roleIds = userRoles.Select(q => q.Id).ToList();
            var users = new[]
            {
                new User
                {
                    Name = "User A",
                    UserRoles = roleIds,
                    Shard = mainShard
                },
                new User
                {
                    Name = "User B",
                    UserRoles = new List<Guid> {roleIds[0]},
                    Shard = mainShard
                }
            };
            var userIds = users.Select(q => q.Id).ToList();
            using (var session = documentStore.OpenSession())
            {
                foreach (var user in users)
                {
                    session.Store(user);
                }
                foreach (var userRole in userRoles)
                {
                    session.Store(userRole);
                }
                session.SaveChanges();
            }

            var type = typeof (UserRole);
            var keys =
                roleIds.Select(
                    q => documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier(q, type, false)).ToList();

            using (var session = documentStore.OpenSession())
            {
                var userRolesFromDb = session.Load<UserRole>(roleIds.Cast<ValueType>());

                foreach (var key in keys)
                {
                    Assert.True(session.Advanced.IsLoaded(key));
                }

                Assert.NotEmpty(userRolesFromDb);
                foreach (var userRole in userRolesFromDb)
                {
                    Assert.NotNull(userRole);
                }

                var usersFromDb =
                    session.Include<User, UserRole>(x => x.UserRoles).Load(userIds.Cast<ValueType>());
                Assert.NotEmpty(usersFromDb);
                foreach (var user in usersFromDb)
                {
                    Assert.NotNull(user);
                }

                foreach (var key in keys)
                {
                    Assert.True(session.Advanced.IsLoaded(key));
                }
                var userRolesFromSession = session.Load<UserRole>(roleIds.Cast<ValueType>());

                Assert.NotEmpty(userRolesFromSession);
                foreach (var userRole in userRolesFromSession)
                {
                    Assert.NotNull(userRole);
                }
            }
        }

        private void AssertLoadMultipleUsersWithIncludesAsync(IDocumentStore documentStore, string mainShard)
        {
            var userRoles = new[]
            {
                new UserRole {Name = "User Role A", Shard = mainShard},
                new UserRole {Name = "User Role B", Shard = mainShard}
            };
            var roleIds = userRoles.Select(q => q.Id).ToList();
            var users = new[]
            {
                new User
                {
                    Name = "User A",
                    UserRoles = roleIds,
                    Shard = mainShard
                },
                new User
                {
                    Name = "User B",
                    UserRoles = new List<Guid> {roleIds[0]},
                    Shard = mainShard
                }
            };
            var userIds = users.Select(q => q.Id).ToList();
            using (var session = documentStore.OpenSession())
            {
                foreach (var user in users)
                {
                    session.Store(user);
                }
                foreach (var userRole in userRoles)
                {
                    session.Store(userRole);
                }
                session.SaveChanges();
            }

            var type = typeof (UserRole);
            var keys =
                roleIds.Select(
                    q => documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier(q, type, false)).ToList();

            AsyncHelpers.RunSync(async () =>
            {
                using (var session = documentStore.OpenAsyncSession())
                {
                    var userRolesFromDb = await session.LoadAsync<UserRole>(roleIds.Cast<ValueType>());

                    foreach (var key in keys)
                    {
                        Assert.True(session.Advanced.IsLoaded(key));
                    }

                    Assert.NotEmpty(userRolesFromDb);
                    foreach (var userRole in userRolesFromDb)
                    {
                        Assert.NotNull(userRole);
                    }

                    var usersFromDb =
                        await session.Include<User, UserRole>(x => x.UserRoles).LoadAsync(userIds.Cast<ValueType>());
                    Assert.NotEmpty(usersFromDb);
                    foreach (var user in usersFromDb)
                    {
                        Assert.NotNull(user);
                    }

                    foreach (var key in keys)
                    {
                        Assert.True(session.Advanced.IsLoaded(key));
                    }
                    var userRolesFromSession = await session.LoadAsync<UserRole>(roleIds.Cast<ValueType>());

                    Assert.NotEmpty(userRolesFromSession);
                    foreach (var userRole in userRolesFromSession)
                    {
                        Assert.NotNull(userRole);
                    }
                }
            });
        }

        private void AssertLoadUsersWithIncludes(IDocumentStore documentStore, string mainShard)
        {
            var userRoles = new[]
            {
                new UserRole {Name = "User Role A", Shard = mainShard},
                new UserRole {Name = "User Role B", Shard = mainShard}
            };
            var user = new User
            {
                Name = "User A",
                UserRoles = userRoles.Select(q => q.Id).ToList(),
                Shard = mainShard
            };

            var userId = user.Id;
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

                var userFromSession = session.Load<User>(userId);

                Assert.NotNull(userFromDb);
                Assert.NotNull(userFromSession);
                Assert.NotNull(session.Load<UserRole>(userFromSession.UserRoles.First()));
                Assert.NotNull(session.Load<UserRole>(userFromSession.UserRoles.Last()));
            }
        }

        private void AssertLoadUsersWithIncludesAsync(IDocumentStore documentStore, string mainShard)
        {
            var userRoles = new[]
            {
                new UserRole {Name = "User Role A", Shard = mainShard},
                new UserRole {Name = "User Role B", Shard = mainShard}
            };
            var user = new User
            {
                Name = "User A",
                UserRoles = userRoles.Select(q => q.Id).ToList(),
                Shard = mainShard
            };

            var userId = user.Id;
            using (var session = documentStore.OpenSession())
            {
                session.Store(user);
                foreach (var userRole in userRoles)
                {
                    session.Store(userRole);
                }
                session.SaveChanges();
            }

            AsyncHelpers.RunSync(async() =>
            {
                using (var session = documentStore.OpenAsyncSession())
                {
                    var userFromDb = await session.Include<User, UserRole>(x => x.UserRoles).LoadAsync(userId);

                    var userFromSession = await session.LoadAsync<User>(userId);

                    Assert.NotNull(userFromDb);
                    Assert.NotNull(userFromSession);
                    Assert.NotNull(await session.LoadAsync<UserRole>(userFromSession.UserRoles.First()));
                    Assert.NotNull(await session.LoadAsync<UserRole>(userFromSession.UserRoles.Last()));
                }
            });
        }

        public void TestWithShardedStore(Action<ShardedDocumentStore, string> action, string mainShard)
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
            }.ShardingOn<User>(x => x.Shard).ShardingOn<UserRole>(x => x.Shard)))
            {
                documentStore.Initialize();
                action(documentStore, mainShard);
            }
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
            public string Shard { get; set; }
        }

        public class UserRole : IdObject
        {
            public string Name { get; set; }
            public string Shard { get; set; }
        }
    }
}
