using System;
using System.Threading;
using FastTests;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class BasicRavenEtlTests : RavenTestBase
    {
        [Fact]
        public void Basic_etl_test()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var database = GetDatabase(src.DefaultDatabase).Result;

                var mre = new ManualResetEventSlim();
                database.EtlLoader.BatchCompleted += (n, s) =>
                {
                    if (s.LoadSuccesses == 1)
                        mre.Set();
                };

                using (var session = src.OpenSession())
                {
                    session.Store(new EtlConfiguration()
                    {
                        RavenTargets =
                        {
                            new RavenEtlConfiguration()
                            {
                                Name = "basic test",
                                Url = dest.Url,
                                Database = dest.DefaultDatabase,
                                Collection = "Users",
                                Script = "this.Name = 'James Doe';"
                            }
                        }
                    }, "Raven/ETL");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    });

                    session.SaveChanges();
                }

                mre.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("James Doe", user.Name);
                }
            }
        }
    }
}