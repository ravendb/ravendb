using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_542 : RavenTestBase
    {
        [Fact]
        public void MapWithMinValueComparison()
        {
            using (var store = GetDocumentStore())
            {
                new OrganizationIndex().Execute(store);

                using (var session = store.OpenSession())
                {

                    var orgs = from i in Enumerable.Range(1, 10)
                               select new Organization
                               {
                                   Id = i.ToString(),
                                   DateApproved = DateTime.Now,
                                   Name = "org" + i
                               };

                    foreach (var org in orgs)
                        session.Store(org);
                    session.SaveChanges();

                    store.Operations.Send(new PatchOperation("organizations/1", null,
                        new PatchRequest
                        {
                            Values =
                            {
                                {"DateApproved", "2012-09-07T09:41:42.9893269"},
                                {"NewProp", "test"}
                            },
                            Script = "this.DateApproved = DateApproved; this['NewProp'] = NewProp;"
                        }));

                    WaitForIndexing(store);


                    RavenTestHelper.AssertNoIndexErrors(store);
                }
            }
        }

        private class Organization
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public DateTime DateApproved { get; set; }
        }

        private class OrganizationIndex : AbstractIndexCreationTask<Organization, OrganizationIndex.Result>
        {
            public class Result
            {
                public bool IsApproved { get; set; }
            }

            public OrganizationIndex()
            {
                Map = orgs => orgs.Select(org => new
                {
                    org.Name,
                    IsApproved = org.DateApproved == DateTime.MinValue
                });
            }
        }
    }
}
