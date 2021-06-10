using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ETL;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_6711_RavenEtl : EtlTestBase
    {
        public RavenDB_6711_RavenEtl(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Error_if_script_has_both_apply_to_all_documents_and_collections_specified()
        {
            var config = new RavenEtlConfiguration
            {
                Name = "test",
                ConnectionStringName = "test",
                Transforms =
                {
                    new Transformation
                    {
                        Name = "test",
                        ApplyToAllDocuments = true,
                        Collections = {"Users"}
                    }
                }
            };

            config.Initialize(new RavenConnectionString() { Database = "Foo", TopologyDiscoveryUrls = new[] { "http://localhost:8080" } });

            List<string> errors;
            config.Validate(out errors);

            Assert.Equal(1, errors.Count);

            Assert.Equal("Collections cannot be specified when ApplyToAllDocuments is set. Script name: 'test'", errors[0]);
        }

        [Fact]
        public void No_script_and_applied_to_all_documents()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses >= 3);

                AddEtl(src, dest, collections: new string[0], script: null, applyToAllDocuments: true);

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe Doe"
                    });

                    session.Store(new Address
                    {
                        City = "New York"
                    });

                    session.Store(new Order
                    {
                        Id = "orders/1-A", // so we won't generate HiLo
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{Product = "Milk", Quantity = 3},
                            new OrderLine{Product = "Bear", Quantity = 2},
                        }
                    });

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(5, stats.CountOfDocuments); // 3 docs and 2 HiLo 

                    var user = session.Load<User>("users/1-A");
                    Assert.NotNull(user);

                    var address = session.Load<Address>("addresses/1-A");
                    Assert.NotNull(address);

                    var order = session.Load<Order>("orders/1-A");
                    Assert.NotNull(order);
                }

                // update

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    user.Name = "James Doe";

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(5, stats.CountOfDocuments); // 3 docs and 2 HiLo 

                    var user = session.Load<User>("users/1-A");
                    Assert.Equal("James Doe", user.Name);
                }

                // delete

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    session.Delete(user);

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(4, stats.CountOfDocuments); // 2 docs and 2 HiLo 

                    var user = session.Load<User>("users/1-A");
                    Assert.Null(user);
                }
            }
        }

        [Fact]
        public void Script_defined_for_all_documents()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses >= 3);

                AddEtl(src, dest, collections: new string[0], script: "loadToAuditItems({DocumentId:id(this)})", applyToAllDocuments: true);

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe Doe"
                    });

                    session.Store(new Address
                    {
                        City = "New York"
                    });

                    session.Store(new Order
                    {
                        Id = "orders/1-A", // so we won't generate HiLo
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{Product = "Milk", Quantity = 3},
                            new OrderLine{Product = "Bear", Quantity = 2},
                        }
                    });

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(3, stats.CountOfDocuments);

                    var audit = session.Advanced.LoadStartingWith<AuditItem>("users/1-A/AuditItems/")[0];
                    Assert.NotNull(audit);
                    Assert.Equal("users/1-A", audit.DocumentId);

                    audit = session.Advanced.LoadStartingWith<AuditItem>("addresses/1-A/AuditItems/")[0];
                    Assert.NotNull(audit);
                    Assert.Equal("addresses/1-A", audit.DocumentId);

                    audit = session.Advanced.LoadStartingWith<AuditItem>("orders/1-A/AuditItems/")[0];
                    Assert.NotNull(audit);
                    Assert.Equal("orders/1-A", audit.DocumentId);
                }

                // update

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    user.Name = "James Doe";

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(3, stats.CountOfDocuments);
                }

                // delete

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    session.Delete(user);

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);
                }
            }
        }

        [Fact]
        public void Script_defined_for_all_documents_with_filtering_and_loads_to_the_same_collection_for_some_docs()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses >= 3);

                AddEtl(src, dest, collections: new string[0], script: @"
if (this['@metadata']['@collection'] != 'Orders')
    loadToPeople({Name: this.Name})"
                    , applyToAllDocuments: true);

                using (var session = src.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Agent Smith"
                    });

                    session.Store(new User
                    {
                        Name = "Neo"
                    });

                    session.Store(new Order
                    {
                        Id = "orders/1-A"
                    });

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);

                    var smith = session.Load<Person>("people/1-A");
                    Assert.NotNull(smith);

                    var neo = session.Advanced.LoadStartingWith<Person>("users/1-A/people")[0];
                    Assert.NotNull(neo);
                }

                // update

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    user.Name = "James Doe";

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);
                }

                // delete

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    session.Delete(user);

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                {
                    var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                    Assert.Equal(1, stats.CountOfDocuments);
                }
            }
        }

        private class AuditItem
        {
            public string Id { get; set; }

            public string DocumentId { get; set; }
        }
    }
}
