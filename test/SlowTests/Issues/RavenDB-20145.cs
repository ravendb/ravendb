using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Xunit;
using Raven.Client.Documents.Indexes;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20145 : RavenTestBase
    {
        public RavenDB_20145(ITestOutputHelper output) : base(output)
        {

        }

        [Fact]
        public void CanCreateIndex()
        {
            using var store = GetDocumentStore();
            new CustomIndex().Execute(store);

            using (var s = store.OpenSession())
            {
                s.Store(new CustomClass
                {
                    NestedProperties = new Dictionary<string, Dictionary<string, object>>
                    {
                        ["DisplayName"] = new Dictionary<string, object>
                        {
                            ["da-DK"] = "foo",
                            ["en-US"] = "bar"
                        }
                    },
                    SimpleProperties = new Dictionary<string, object>
                    {
                        ["DisplayName"] = "snap"
                    }
                });
                s.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            using (var s = store.OpenSession())
            {
                var q = s.Advanced.RawQuery<CustomClass>("from index CustomIndex where WorkingDisplayName = 'snap'")
                    .ToList();
                Assert.NotEmpty(q);
                q = s.Advanced.RawQuery<CustomClass>("from index CustomIndex where BrokenDisplayName = 'foo'")
                    .ToList();
                Assert.NotEmpty(q);
            }

        }

        private class CustomIndex : AbstractIndexCreationTask<CustomClass>
        {
            public override string IndexName => "CustomIndex";

            public CustomIndex()
            {

                Map = customObjects =>
                    from obj in customObjects
                    select new
                    {
                        BrokenDisplayNameCollection = obj.NestedProperties["DisplayName"],
                        BrokenDisplayName = obj.NestedProperties["DisplayName"]["da-DK"],
                        WorkingDisplayName = obj.SimpleProperties["DisplayName"]
                    };
            }
        }

        private class CustomClass
        {
            public Dictionary<string, Dictionary<string, object>> NestedProperties { get; set; }
            public Dictionary<string, object> SimpleProperties { get; set; }
        }

    }
}
