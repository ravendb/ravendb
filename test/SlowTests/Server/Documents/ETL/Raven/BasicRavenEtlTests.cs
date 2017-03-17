using System;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class BasicRavenEtlTests : RavenTestBase
    {
        [Fact]
        public void Simple_script()
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

                SetupEtl(src, new EtlConfiguration
                {
                    RavenTargets =
                    {
                        new RavenEtlConfiguration
                        {
                            Name = "basic test",
                            Url = dest.Url,
                            Database = dest.DefaultDatabase,
                            Collection = "Users",
                            Script = "this.Name = 'James Doe';"
                        }
                    }
                });

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

        [Fact]
        public void No_script()
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

                SetupEtl(src, new EtlConfiguration
                {
                    RavenTargets =
                        {
                            new RavenEtlConfiguration
                            {
                                Name = "basic test",
                                Url = dest.Url,
                                Database = dest.DefaultDatabase,
                                Collection = "Users"
                            }
                        }
                });

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
                    Assert.Equal("Joe Doe", user.Name);
                }
            }
        }

        private static void SetupEtl(DocumentStore src, EtlConfiguration configuration)
        {
            using (var session = src.OpenSession())
            {
                session.Store(configuration, "Raven/ETL");

                session.SaveChanges();
            }
        }
    }
}