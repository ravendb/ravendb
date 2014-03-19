using System;
using System.Threading;
using System.Transactions;
using NLog;
using NLog.Targets;
using Raven.Client.Document;
using Xunit;
using log4net.Appender;
using log4net.Layout;

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
                    Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
                                                      new ManyDocumentsViaDTC.DummyEnlistmentNotification(),
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

    public class DtcBluesRemote : RemoteClientTest
    {
        [Fact]
        public void CanQueryDtcForUncommittedItem()
        {
            using (var server = GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
            {

                EnsureDtcIsSupported(server);

                for (int i = 0; i < 150; i++)
                {
                    string id;
                    using (var tx = new TransactionScope())
                    {
                        Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
                                                          new ManyDocumentsViaDTC.DummyEnlistmentNotification(),
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

    public class DtcBluesRemoteAndTouchingTheDisk : RemoteClientTest
    {
        [Fact]
        public void CanQueryDtcForUncommittedItem()
        {
            using (var server = GetNewServer(runInMemory: false))
            using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
            {

                EnsureDtcIsSupported(server);

                for (int i = 0; i < 150; i++)
                {
                    string id;
                    using (var tx = new TransactionScope())
                    {
                        Transaction.Current.EnlistDurable(ManyDocumentsViaDTC.DummyEnlistmentNotification.Id,
                                                          new ManyDocumentsViaDTC.DummyEnlistmentNotification(),
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