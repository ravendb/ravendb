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

        [Fact]
        public void Filtering_with_null_and_false_and_transformation_to_different_object_with_load_document()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupEtl(src, dest, "users", @"
if (this.Age % 4 == 0) 
    return null; 
else if (this.Age % 2 == 0) 
    return false;
else 
    return {Name: this.Name + ' ' + this.LastName, Address:LoadDocument(this.AddressId)};
");

                const int count = 30;

                using (var session = src.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        session.Store(new User
                        {
                            Age = i,
                            Name = "James",
                            LastName = "Smith",
                            AddressId = $"addresses/{i}"
                        }, "users/" + i);

                        session.Store(new Address
                        {
                            City = "New York"
                        }, $"addresses/{i}");
                    }

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    var loaded = 0;

                    for (int i = 0; i < count; i++)
                    {
                        var user = session.Load<UserWithAddress>("users/" + i);
                        
                        if (i % 2 == 0)
                        {
                            Assert.Null(user);
                        }
                        else
                        {
                            Assert.Equal("New York", user.Address.City);
                            loaded++;
                        }
                    }

                    Assert.Equal(15, loaded);
                }
            }
        }

        private class UserWithAddress : User
        {
            public Address Address { get; set; }
        }
    }
}