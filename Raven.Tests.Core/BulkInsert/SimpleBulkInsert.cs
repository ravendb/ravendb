// -----------------------------------------------------------------------
//  <copyright file="SimpleBulkInsert.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Core.BulkInsert
{
    public class SimpleBulkInsert : RavenCoreTestBase
    {
#if DNXCORE50
        public SimpleBulkInsert(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        [Theory]
#if !DNXCORE50
        [PropertyData("InsertOptions")]
#else
        [MemberData("InsertOptions")]
#endif
        public void BasicBulkInsert(BulkInsertOptions options)
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert(options: options))
                {
                    for (int i = 0; i < 100; i++)
                    {
                        bulkInsert.Store(new User { Name = "User - " + i });
                    }
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Advanced.LoadStartingWith<User>("users/", pageSize: 128);
                    Assert.Equal(100, users.Length);
                }
            }
        }

        [Theory]
#if !DNXCORE50
        [PropertyData("InsertOptions")]
#else
        [MemberData("InsertOptions")]
#endif
        public void BulkInsertShouldNotOverwriteWithOverwriteExistingSetToFalse(BulkInsertOptions options)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "users/1",
                        Name = "User - 1"
                    });

                    session.SaveChanges();
                }

                options.OverwriteExisting = false;

                var e = Assert.Throws<ConcurrencyException>(() =>
                {
                    using (var bulkInsert = store.BulkInsert(options: options))
                    {
                        for (var i = 0; i < 10; i++)
                        {
                            bulkInsert.Store(new User
                            {
                                Id = "users/" + (i + 1),
                                Name = "resU - " + (i + 1)
                            });
                        }
                    }
                });

                Assert.Contains("users/1", e.Message);

                using (var session = store.OpenSession())
                {
                    var users = session.Advanced.LoadStartingWith<User>("users/", pageSize: 128);
                    Assert.Equal(1, users.Length);
                    Assert.True(users.All(x => x.Name.StartsWith("User")));
                }
            }
        }

        [Theory]
#if !DNXCORE50
        [PropertyData("InsertOptions")]
#else
        [MemberData("InsertOptions")]
#endif
        public void BulkInsertShouldOverwriteWithOverwriteExistingSetToTrue(BulkInsertOptions options)
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        bulkInsert.Store(new User
                        {
                            Id = "users/" + (i + 1),
                            Name = "User - " + (i + 1)
                        });
                    }
                }

                options.OverwriteExisting = true;

                using (var bulkInsert = store.BulkInsert(options: options))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        bulkInsert.Store(new User
                        {
                            Id = "users/" + (i + 1),
                            Name = "resU - " + (i + 1)
                        });
                    }
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Advanced.LoadStartingWith<User>("users/", pageSize: 128);
                    Assert.Equal(10, users.Length);
                    Assert.True(users.All(x => x.Name.StartsWith("resU")));
                }
            }
        }

        [Theory]
#if !DNXCORE50
        [PropertyData("InsertOptions")]
#else
        [MemberData("InsertOptions")]
#endif
        public void StoreWithSpacesInDocumentId(BulkInsertOptions options)
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert(options: options))
                {
                    bulkInsert.Store(new User { Name = "Id With Spaces" },"users/12       ");    
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Load<User>("users/12       ");
                    Assert.Equal(users.Name, "Id With Spaces");
                }
            }
        }
    }
}
