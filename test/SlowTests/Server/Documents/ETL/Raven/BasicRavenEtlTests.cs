using System;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class BasicRavenEtlTests : EtlTestBase
    {
        [Fact]
        public void Simple_script()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
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

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses == 1);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    });

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

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

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses == 1);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    });

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);
                }
            }
        }
    }
}