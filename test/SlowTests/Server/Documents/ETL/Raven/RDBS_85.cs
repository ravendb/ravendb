using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RDBS_85 : EtlTestBase
    {
        public RDBS_85(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MustNotSkipAnyDocumentsIfTaskIsCanceledDuringLoad()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                AddEtl(src, dst, "users",
                    @"loadToUsers(this)");

                var srcDb = await GetDatabase(src.Database);

                var process = (RavenEtl)srcDb.EtlLoader.Processes.First();

                var afterStop = new AsyncManualResetEvent();

                process.BeforeActualLoad += p =>
                {
                    p.Stop("testing");
                    afterStop.Set();
                };

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Arek"
                    }, "users/1");

                    session.SaveChanges();
                }

                Assert.True(await afterStop.WaitAsync(TimeSpan.FromSeconds(30)));

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/2");

                    session.SaveChanges();
                }

                process.BeforeActualLoad = null;

                process.Start();

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dst.OpenSession())
                {
                    User[] users = session.Advanced.LoadStartingWith<User>("users/");

                    Assert.Equal(2, users.Length);

                    var user = session.Load<User>("users/2");
                    Assert.NotNull(user);

                    user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }
            }
        }

        [Fact]
        public async Task MustNotSkipAnyDocumentsIfLoadFailsAndThereWereSomeInternalFiltering()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                AddEtl(src, dst, new string[0],
                    @"loadToUsers(this)", applyToAllDocuments: true); // this will filter our HiLo docs under the covers

                var srcDb = await GetDatabase(src.Database);

                var process = (RavenEtl)srcDb.EtlLoader.Processes.First();

                var afterLoadError = new AsyncManualResetEvent();

                process.BeforeActualLoad += p =>
                {
                    afterLoadError.Set();
                    throw new Exception("Force the failure of the load of docs");
                };

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Arek"
                    });

                    session.SaveChanges();
                }

                Assert.True(await afterLoadError.WaitAsync(TimeSpan.FromSeconds(30)));
                
                process.BeforeActualLoad = null;

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    });

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dst.OpenSession())
                {
                    User[] users = session.Advanced.LoadStartingWith<User>("users/");

                    Assert.Equal(2, users.Length);
                }
            }
        }
    }
}
