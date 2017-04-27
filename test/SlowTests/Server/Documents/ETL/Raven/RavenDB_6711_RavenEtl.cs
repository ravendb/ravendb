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
    public class RavenDB_6711_RavenEtl : EtlTestBase
    {
        [Fact]
        public void Error_if_script_has_both_apply_to_all_documents_and_collections_specified()
        {
            var config = new EtlConfiguration<RavenDestination>()
            {
                Destination = new RavenDestination
                {
                    Url = "http://localhost:8080",
                    Database = "Northwind",
                },
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

            List<string> errors;
            config.Validate(out errors);

            Assert.Equal(1, errors.Count);

            Assert.Equal("Collections cannot be specified when ApplyToAllDocuments is set", errors[0]);
        }

        [Fact]
        public void No_script_and_applied_to_all_documents()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses >= 3);

                SetupEtl(src, dest, collections: new string[0], script: null, applyToAllDocuments: true);

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
                        Id = "orders/1", // so we won't generate HiLo
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
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

                    Assert.Equal(5, stats.CountOfDocuments); // 3 docs and 2 HiLo 

                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);

                    var address = session.Load<Address>("addresses/1");
                    Assert.NotNull(address);

                    var order = session.Load<Order>("orders/1");
                    Assert.NotNull(order);
                }

                // update

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    user.Name = "James Doe";

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

                    Assert.Equal(5, stats.CountOfDocuments); // 3 docs and 2 HiLo 

                    var user = session.Load<User>("users/1");
                    Assert.Equal("James Doe", user.Name);
                }

                // delete

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    session.Delete(user);

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

                    Assert.Equal(4, stats.CountOfDocuments); // 2 docs and 2 HiLo 

                    var user = session.Load<User>("users/1");
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

                SetupEtl(src, dest, collections: new string[0], script: "loadToAuditItems({DocumentId:__document_id})", applyToAllDocuments: true);

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
                        Id = "orders/1", // so we won't generate HiLo
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
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

                    Assert.Equal(3, stats.CountOfDocuments);

                    var audit = session.Advanced.LoadStartingWith<AuditItem>("users/1/AuditItems/")[0];
                    Assert.NotNull(audit);
                    Assert.Equal("users/1", audit.DocumentId);

                    audit = session.Advanced.LoadStartingWith<AuditItem>("addresses/1/AuditItems/")[0];
                    Assert.NotNull(audit);
                    Assert.Equal("addresses/1", audit.DocumentId);

                    audit = session.Advanced.LoadStartingWith<AuditItem>("orders/1/AuditItems/")[0];
                    Assert.NotNull(audit);
                    Assert.Equal("orders/1", audit.DocumentId);
                }

                // update

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    user.Name = "James Doe";

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                {
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

                    Assert.Equal(3, stats.CountOfDocuments);
                }

                // delete

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    session.Delete(user);

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

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

                SetupEtl(src, dest, collections: new string[0], script: @"
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
                        Id = "orders/1"
                    });

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);

                    var smith = session.Load<Person>("people/1");
                    Assert.NotNull(smith);

                    var neo = session.Advanced.LoadStartingWith<Person>("users/1/people")[0];
                    Assert.NotNull(neo);
                }

                // update

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    user.Name = "James Doe";

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                {
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);
                }

                // delete

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    session.Delete(user);

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                {
                    var stats = dest.Admin.Send(new GetStatisticsOperation());

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