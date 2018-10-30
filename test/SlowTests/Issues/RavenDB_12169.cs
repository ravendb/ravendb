using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12169 : RavenTestBase
    {
        [Fact]
        public void CanUseBatchPatchCommand()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Id = "companies/1",
                        Name = "C1"
                    });

                    session.Store(new Company
                    {
                        Id = "companies/2",
                        Name = "C2"
                    });

                    session.Store(new Company
                    {
                        Id = "companies/3",
                        Name = "C3"
                    });

                    session.Store(new Company
                    {
                        Id = "companies/4",
                        Name = "C4"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var c1 = session.Load<Company>("companies/1");
                    var c2 = session.Load<Company>("companies/2");
                    var c3 = session.Load<Company>("companies/3");
                    var c4 = session.Load<Company>("companies/4");

                    Assert.Equal("C1", c1.Name);
                    Assert.Equal("C2", c2.Name);
                    Assert.Equal("C3", c3.Name);
                    Assert.Equal("C4", c4.Name);

                    var ids = new List<string>
                    {
                        c1.Id,
                        c3.Id
                    };

                    session.Advanced.Defer(new BatchPatchCommandData(ids, new PatchRequest
                    {
                        Script = "this.Name = 'test';"
                    }, null));

                    session.Advanced.Defer(new BatchPatchCommandData(new List<string> { c4.Id }, new PatchRequest
                    {
                        Script = "this.Name = 'test2';"
                    }, null));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var c1 = session.Load<Company>("companies/1");
                    var c2 = session.Load<Company>("companies/2");
                    var c3 = session.Load<Company>("companies/3");
                    var c4 = session.Load<Company>("companies/4");

                    Assert.Equal("test", c1.Name);
                    Assert.Equal("C2", c2.Name);
                    Assert.Equal("test", c3.Name);
                    Assert.Equal("test2", c4.Name);
                }

                using (var session = store.OpenSession())
                {
                    var c2 = session.Load<Company>("companies/2");

                    session.Advanced.Defer(new BatchPatchCommandData(new List<(string Id, string ChangeVector)> { (c2.Id, "invalidCV") }, new PatchRequest
                    {
                        Script = "this.Name = 'test2';"
                    }, null));

                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }

                using (var session = store.OpenSession())
                {
                    var c1 = session.Load<Company>("companies/1");
                    var c2 = session.Load<Company>("companies/2");
                    var c3 = session.Load<Company>("companies/3");
                    var c4 = session.Load<Company>("companies/4");

                    Assert.Equal("test", c1.Name);
                    Assert.Equal("C2", c2.Name);
                    Assert.Equal("test", c3.Name);
                    Assert.Equal("test2", c4.Name);
                }
            }
        }
    }
}
