// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1533.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1533 : RavenTest
    {
        class Developer
        {
            public string Nick { get; set; }
            public int Id { get; set; }
        }

        [Fact]
        public void
           CanDeleteObjectByKeyOnSyncSession()
        {
            using (var store = NewDocumentStore())
            {
                string developer1Id;
                var developer1 = new Developer
                {
                    Nick = "ayende"
                };
                using (var session = store.OpenSession())
                {
                    session.Store(developer1);
                    session.SaveChanges();
                    developer1Id = store.Conventions.FindFullDocumentKeyFromNonStringIdentifier(developer1.Id,
                                                                                                typeof(Developer),
                                                                                                false);
                }

                using (var session = (DocumentSession)store.OpenSession())
                {
                    Assert.False(session.IsDeleted(developer1Id));
                    session.Delete(developer1Id);
                    Assert.True(session.IsDeleted(developer1Id));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Developer>(developer1.Id));
                }
            }
        }

        [Fact]
        public void
           CanDeleteObjectByTypeAndIdOnSyncSession()
        {
            using (var store = NewDocumentStore())
            {
                string developer1Id;
                var developer1 = new Developer
                {
                    Nick = "ayende"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(developer1);
                    session.SaveChanges();
                    developer1Id = store.Conventions.FindFullDocumentKeyFromNonStringIdentifier(developer1.Id,
                                                                                                typeof(Developer),
                                                                                                false);
                }

                using (var session = (DocumentSession)store.OpenSession())
                {
                    Assert.False(session.IsDeleted(developer1Id));
                    session.Delete<Developer>(developer1.Id);
                    Assert.True(session.IsDeleted(developer1Id));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Developer>(developer1.Id));
                }
            }
        }

        [Fact]
        public async Task
           CanDeleteObjectByTypeAndIdOnAsyncSession()
        {
            using (var store = NewDocumentStore())
            {
                string developer1Id;
                var developer1 = new Developer
                {
                    Nick = "ayende"
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(developer1);
                    await session.SaveChangesAsync();
                    developer1Id = store.Conventions.FindFullDocumentKeyFromNonStringIdentifier(developer1.Id,
                                                                                                typeof(Developer),
                                                                                                false);
                }

                using (var session = (AsyncDocumentSession)store.OpenAsyncSession())
                {
                    Assert.False(session.IsDeleted(developer1Id));
                    session.Delete<Developer>(developer1.Id);
                    Assert.True(session.IsDeleted(developer1Id));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Null(await session.LoadAsync<Developer>(developer1.Id));
                }
            }
        }

        [Fact]
        public async Task
           CanDeleteObjectByKeyOnAsyncSession()
        {
            using (var store = NewDocumentStore())
            {
                string developer1Id;
                var developer1 = new Developer
                {
                    Nick = "ayende"
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(developer1);
                    await session.SaveChangesAsync();
                    developer1Id = store.Conventions.FindFullDocumentKeyFromNonStringIdentifier(developer1.Id,
                                                                                                typeof(Developer),
                                                                                                false);
                }

                using (var session = (AsyncDocumentSession)store.OpenAsyncSession())
                {
                    Assert.False(session.IsDeleted(developer1Id));
                    session.Delete(developer1Id);
                    Assert.True(session.IsDeleted(developer1Id));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Null(await session.LoadAsync<Developer>(developer1.Id));
                }
            }
        }

        [Fact]
        public void ShouldNotThrowWhenDeletingUnchangedLoadedObject()
        {
            using (var store = NewDocumentStore())
            {
                var developer1 = new Developer
                {
                    Nick = "ayende"
                };
                string developer1Id;

                using (var session = (DocumentSession) store.OpenSession())
                {
                    session.Store(developer1);
                    session.SaveChanges();
                    developer1Id = store.Conventions.FindFullDocumentKeyFromNonStringIdentifier(developer1.Id,
                                                                                                      typeof(Developer),
                                                                                                      false);
                }

                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Load<Developer>(developer1.Id);
                    Assert.True(session.IsLoaded(developer1Id));
                    session.Delete<Developer>(developer1.Id);
                    Assert.False(session.IsLoaded(developer1Id));
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void ShouldThrowWhenDeletingChangedLoadedObject()
        {
            using (var store = NewDocumentStore())
            {
                var developer1 = new Developer
                {
                    Nick = "ayende"
                };

                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(developer1);
                    session.SaveChanges();
                }

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var newDeveloper = session.Load<Developer>(developer1.Id);
                    // modify object
                    newDeveloper.Nick = "newNick";
                    Assert.Throws<InvalidOperationException>(() => session.Delete<Developer>(developer1.Id));
                }
            }
        }

        [Fact]
        public void ShouldThrowWhenDeletingNewlyCreatedEntity()
        {
            using (var store = NewDocumentStore())
            {
                var developer1 = new Developer
                {
                    Nick = "ayende"
                };

                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(developer1);
                    var developer1Id = store.Conventions.FindFullDocumentKeyFromNonStringIdentifier(developer1.Id,
                                                                                                       typeof(Developer),
                                                                                                       false);
                    Assert.Throws<InvalidOperationException>(() => session.Delete<Developer>(developer1.Id));
                }
            }
        }

        [Fact]
        public void ShouldThrowWhenStoringJustDeletedIdentifier()
        {
            using (var store = NewDocumentStore())
            {
                var developer1 = new Developer
                {
                    Nick = "ayende",
                    Id = 11
                };

                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Delete<Developer>(11);
                    Assert.Throws<InvalidOperationException>(() => session.Store(developer1));
                }
            }
        }
    }
}