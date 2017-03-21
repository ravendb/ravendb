using System;
using System.Collections.Generic;
using System.Threading;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations;
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
                            Script = @"this.Name = 'James Doe';
                                       loadToUsers(this);"
                        }
                    }
                });

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Null(user);
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

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

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

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Null(user);
                }
            }
        }

        [Fact]
        public void Filtering_and_transformation_with_load_document()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupEtl(src, dest, "users", @"
if (this.Age % 4 == 0) 
    return;
else if (this.Age % 2 == 0) 
    return;

loadToUsers(
    {
        Name: this.Name + ' ' + this.LastName, 
        Address: LoadDocument(this.AddressId)
    });
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

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    for (var i = 0; i < count; i++)
                    {
                        session.Delete($"users/{i}");
                    }

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        var user = session.Load<UserWithAddress>("users/" + i);

                        Assert.Null(user);
                    }
                }
            }
        }

        [Fact]
        public void Loading_to_different_collections()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupEtl(src, dest, "users", @"
loadToUsers(this);
loadToPeople({Name: this.Name + ' ' + this.LastName });
loadToAddresses(LoadDocument(this.AddressId));
");
                const int count = 5;

                using (var session = src.OpenSession())
                {
                    for (int i = 1; i <= count; i++)
                    {
                        session.Store(new User
                        {
                            Age = i,
                            Name = "James",
                            LastName = "Smith",
                            AddressId = $"addresses/{i}"
                        });

                        session.Store(new Address
                        {
                            City = "New York"
                        });
                    }

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    for (var i = 1; i <= count; i++)
                    {
                        var user = session.Load<User>("users/1");
                        Assert.NotNull(user);
                        Assert.Equal("James", user.Name);
                        Assert.Equal("Smith", user.LastName);

                        var person = session.Load<Person>($"users/{i}/people/1");
                        Assert.NotNull(person);
                        Assert.Equal("James Smith", person.Name);

                        var address = session.Load<Address>($"users/{i}/addresses/1");
                        Assert.NotNull(address);
                        Assert.Equal("New York", address.City);
                    }
                }

                var stats = dest.Admin.Send(new GetStatisticsOperation());

                Assert.Equal(15, stats.CountOfDocuments);

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/3");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/3");
                    Assert.Null(user);

                    var person = session.Load<Person>("users/3/people/1");
                    Assert.Null(person);

                    var address = session.Load<Address>("users/3/addresses/1");
                    Assert.Null(address);
                }

                stats = dest.Admin.Send(new GetStatisticsOperation());

                Assert.Equal(12, stats.CountOfDocuments);
            }
        }

        private class UserWithAddress : User
        {
            public Address Address { get; set; }
        }
    }
}