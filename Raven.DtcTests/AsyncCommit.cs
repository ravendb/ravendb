//-----------------------------------------------------------------------
// <copyright file="AsyncCommit.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class AsyncCommit : RavenTest
    {
        [Fact]
        public void DtcCommitWillGiveNewResultIfNonAuthoritativeIsSetToFalse()
        {
            using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(documentStore);

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new AccurateCount.User { Name = "Ayende" });
                    s.SaveChanges();
                }

                var task = new Task(() =>
                {
                    using (var s = documentStore.OpenSession())
                    {
                        s.Advanced.AllowNonAuthoritativeInformation = false;
                        var user = s.Load<AccurateCount.User>("users/1");
                        Assert.Equal("Rahien", user.Name);
                    }
                });

                using (var s = documentStore.OpenSession())
                using (var scope = new TransactionScope())
                {
                    var user = s.Load<AccurateCount.User>("users/1");
                    user.Name = "Rahien";
                    s.SaveChanges();
                    task.Start();
                    Assert.False(task.Wait(250, CancellationToken.None));
                    scope.Complete();
                }

                task.Wait();
            }
        }

        [Fact]
        public void DtcCommitWillGiveNewResultIfNonAuthoritativeIsSetToFalse_OnInclude()
        {
            using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(documentStore);

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new AccurateCount.User { Name = "B" }, "users/1");
                    s.Store(new AccurateCount.User { Name = "A", Manager = "users/1"}, "users/2");

                    s.SaveChanges();
                }

                var task = new Task(() =>
                {
                    using (var s = documentStore.OpenSession())
                    {
                        s.Advanced.AllowNonAuthoritativeInformation = false;
                        var user = s.Include<AccurateCount.User>(x=>x.Manager).Load("users/2");
                        var manager = s.Load<AccurateCount.User>(user.Manager);
                        Assert.Equal("Rahien", manager.Name);
                    }
                });

                using (var s = documentStore.OpenSession())
                using (var scope = new TransactionScope())
                {
                    var user = s.Load<AccurateCount.User>("users/1");
                    user.Name = "Rahien";
                    s.SaveChanges();
                    task.Start();
                    Assert.False(task.Wait(250, CancellationToken.None));
                    scope.Complete();
                }

                task.Wait();
            }
        }


        [Fact]
        public void DtcCommitWillGiveNewResultIfNonAuthoritativeIsSetToFalseWhenQuerying()
        {
            using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(documentStore);

                documentStore.DatabaseCommands.PutIndex("test",
                                                        new IndexDefinition
                                                        {
                                                            Map = "from doc in docs select new { doc.Name }"
                                                        });

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new AccurateCount.User { Name = "Ayende" });
                    s.SaveChanges();
                }

                WaitForIndexing(documentStore);

                var task = new Task(() =>
                {
                    using (var s = documentStore.OpenSession())
                    {
                        s.Advanced.AllowNonAuthoritativeInformation = false;
                        var user = s.Advanced.DocumentQuery<AccurateCount.User>("test")
                            .FirstOrDefault();
                        Assert.Equal("Rahien", user.Name);
                    }
                });
                using (var s = documentStore.OpenSession())
                using (var scope = new TransactionScope())
                {
                    var user = s.Load<AccurateCount.User>("users/1");
                    user.Name = "Rahien";
                    s.SaveChanges();
                    task.Start();
                    Assert.False(task.Wait(250, CancellationToken.None));
                    scope.Complete();
                }
                task.Wait();
            }
        }
    }
}
