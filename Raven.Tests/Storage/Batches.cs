using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Storage
{
    public class Batches : RavenTest
    {
        private int commitsCalled = 0;
        public void RegisterNotification(ITransactionalStorage ts, WorkContext wc)
        {
            wc.ShouldNotifyAboutWork(() => "Incremented Commit Count");
        }

        [Theory]
        [PropertyData("Storages")]
        public void BatchNestingAndCommits(string storageType)
        {
            using (var dd = new DocumentDatabase(new RavenConfiguration
            {
                DataDirectory = NewDataPath("DataDirectory"),
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false
            }, null))
            {

                var ts = dd.TransactionalStorage;
                var wc = dd.WorkContext;
                ts.Batch(x => { RegisterNotification(ts,wc); });
                Assert.Equal(1, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();
                ts.Batch(x => { RegisterNotification(ts,wc); ts.Batch(y => { RegisterNotification(ts,wc); }); });
                Assert.Equal(1, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();
                ts.Batch(x =>
                {
                    RegisterNotification(ts,wc);
                    using (ts.DisableBatchNesting())
                    {
                        ts.Batch(y => { RegisterNotification(ts,wc); });
                    }
                    RegisterNotification(ts,wc);
                });
                Assert.Equal(2, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();

                using (ts.DisableBatchNesting())
                {
                    ts.Batch(x => { RegisterNotification(ts,wc); ts.Batch(y => { RegisterNotification(ts,wc); }); });
                }

                Assert.Equal(1, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();

                ts.Batch(x => { RegisterNotification(ts,wc); ts.Batch(y => { RegisterNotification(ts,wc); ts.Batch(z => { RegisterNotification(ts,wc); }); }); });

                Assert.Equal(1, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();

                ts.Batch(x =>
                {
                    RegisterNotification(ts,wc);
                    using (ts.DisableBatchNesting())
                    {
                        ts.Batch(y => { RegisterNotification(ts,wc); ts.Batch(z => { RegisterNotification(ts,wc); }); });
                    }
                    RegisterNotification(ts,wc);
                });
                Assert.Equal(2, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();

                using (ts.DisableBatchNesting())
                {
                    ts.Batch(x => { RegisterNotification(ts,wc); ts.Batch(y => { RegisterNotification(ts,wc); ts.Batch(z => { RegisterNotification(ts,wc); }); }); });
                }
                Assert.Equal(1, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();

                ts.Batch(x =>
                {
                    RegisterNotification(ts,wc);
                    ts.Batch(y =>
                    {
                        RegisterNotification(ts,wc);
                        using (ts.DisableBatchNesting())
                        {
                            ts.Batch(z => { RegisterNotification(ts,wc); });
                        }
                    });
                    RegisterNotification(ts,wc);
                });
                Assert.Equal(2, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();

                ts.Batch(x =>
                {
                    RegisterNotification(ts,wc);
                    ts.Batch(y =>
                    {
                        RegisterNotification(ts,wc);
                        for (var i = 0; i < 10; i++)
                        {
                            ts.Batch(z => { RegisterNotification(ts,wc); });
                        }
                    });
                    RegisterNotification(ts,wc);
                });
                Assert.Equal(1, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();

                using (ts.DisableBatchNesting())
                {
                    ts.Batch(x =>
                    {
                        RegisterNotification(ts,wc);
                        ts.Batch(y =>
                        {
                            RegisterNotification(ts,wc);
                            for (var i = 0; i < 10; i++)
                            {
                                ts.Batch(z => { RegisterNotification(ts,wc); });
                            }
                        });
                        RegisterNotification(ts,wc);
                    });
                }
                Assert.Equal(11, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();


                ts.Batch(x =>
                {
                    RegisterNotification(ts,wc);
                    using (ts.DisableBatchNesting())
                    {
                        RegisterNotification(ts,wc);
                        ts.Batch(y =>
                        {
                            RegisterNotification(ts,wc);
                            for (var i = 0; i < 10; i++)
                            {
                                ts.Batch(z => { RegisterNotification(ts,wc); });
                            }
                            RegisterNotification(ts,wc);
                        });
                    }
                });
                Assert.Equal(12, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();

                ts.Batch(x =>
                {
                    RegisterNotification(ts,wc);
                    ts.Batch(y =>
                    {
                        RegisterNotification(ts,wc);
                        for (var i = 0; i < 10; i++)
                        {
                            RegisterNotification(ts,wc);
                            using (ts.DisableBatchNesting())
                            {
                                ts.Batch(z =>
                                {

                                });
                            }
                            RegisterNotification(ts,wc);
                        }
                    });
                });
                Assert.Equal(1, wc.GetWorkCount() - commitsCalled);
                commitsCalled = wc.GetWorkCount();
            }
        }
    }
}
