using System.Transactions;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class DtcBlues : RavenTest
    {
        [Fact]
        public void CanQueryDtcForUncommittedItem()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(store);

                using (var tx = new TransactionScope())
                {
                    Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id,
                                                      new DummyEnlistmentNotification(),
                                                      EnlistmentOptions.None);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User());
                        session.SaveChanges();
                    }

                    tx.Complete();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.AllowNonAuthoritativeInformation = false;
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }
            }
        }

        [Fact]
        public void NothingToDo_ButCommitIsCAlled()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(store);

                using (var tx = new TransactionScope())
                using (var session = store.OpenSession())
                {
                    session.SaveChanges();
                    tx.Complete();
                }
            }
        }
    }

    public class DtcBluesRemote : RavenTest
    {
        [Fact(Skip = "Run only when needed")]
        public void CanQueryDtcForUncommittedItem()
        {
            using (var server = GetNewServer(requestedStorage: "esent"))
            using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
            {

                EnsureDtcIsSupported(server);

                for (int i = 0; i < 150; i++)
                {
                    string id;
                    using (var tx = new TransactionScope())
                    {
                        Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id,
                                                          new DummyEnlistmentNotification(),
                                                          EnlistmentOptions.None);

                        using (var session = store.OpenSession())
                        {
                            var entity = new User();
                            session.Store(entity);
                            session.SaveChanges();
                            id = entity.Id;
                        }

                        tx.Complete();
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Advanced.AllowNonAuthoritativeInformation = false;
                        var user = session.Load<User>(id);
                        Assert.NotNull(user);
                    }
                }
            }
        }

    }

    public class DtcBluesRemoteAndTouchingTheDisk : RavenTest
    {
        [Fact]
        public void CanQueryDtcForUncommittedItem()
        {
            using (var server = GetNewServer(requestedStorage:"esent"))
            using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
            {

                EnsureDtcIsSupported(server);

                for (int i = 0; i < 150; i++)
                {
                    string id;
                    using (var tx = new TransactionScope())
                    {
                        Transaction.Current.EnlistDurable(DummyEnlistmentNotification.Id,
                                                          new DummyEnlistmentNotification(),
                                                          EnlistmentOptions.None);

                        using (var session = store.OpenSession())
                        {
                            var entity = new User();
                            session.Store(entity);
                            session.SaveChanges();
                            id = entity.Id;
                        }

                        tx.Complete();
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Advanced.AllowNonAuthoritativeInformation = false;
                        var user = session.Load<User>(id);
                        Assert.NotNull(user);
                    }
                }
            }
        }
    }
}
