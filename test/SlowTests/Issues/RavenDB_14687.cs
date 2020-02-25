using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14687 : RavenTestBase
    {
        public RavenDB_14687(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void IndexSpecificSettingShouldBeRespected()
        {
            using (var store = GetDocumentStore())
            {
                var index = new MyJSIndex();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company());

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                IndexErrors[] errors = store.Maintenance.Send(new GetIndexErrorsOperation(new []{index.IndexName}));

                Assert.Equal(1, errors.Length);
                Assert.Equal(0, errors[0].Errors.Length);
            }
        }

        private class MyJSIndex : AbstractJavaScriptIndexCreationTask
        {

            public MyJSIndex()
            {
                Maps = new HashSet<string>()
                {
                    @"
map('Companies', (company) => {
    var x = [];
    for (var i = 0; i < 30000; i++) {
        x.push(i);
    }
    if (company.Address.Country === 'USA') {
        return {
            Name: company.Name,
            Phone: company.Phone,
            City: company.Address.City
        };
    }
})"
                };

                Configuration = new IndexConfiguration() {{RavenConfiguration.GetKey(x => x.Indexing.MaxStepsForScript), "100000"}};
            }
        }
    }
}
