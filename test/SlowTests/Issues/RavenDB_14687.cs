using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Jint.Constraints;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Static;
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
        public async Task IndexSpecificSettingShouldBeRespected()
        {
            var initialMaxStepsForScript = 10;

            using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxStepsForScript)] = initialMaxStepsForScript.ToString() }))
            {
                var index = new MyJSIndex(maxStepsForScript: null);
                index.Execute(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                var indexInstance1 = (MapIndex)database.IndexStore.GetIndex(index.IndexName);
                var compiled1 = (JavaScriptIndex)indexInstance1._compiled;

                Assert.Equal(initialMaxStepsForScript, compiled1._engine.FindConstraint<MaxStatements>().Max);

                const int maxStepsForScript = 1000;
                index = new MyJSIndex(maxStepsForScript);
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                var indexInstance2 = (MapIndex)database.IndexStore.GetIndex(index.IndexName);
                var compiled2 = (JavaScriptIndex)indexInstance2._compiled;

                Assert.NotEqual(indexInstance1, indexInstance2);
                Assert.NotEqual(compiled1, compiled2);

                Assert.Equal(maxStepsForScript, compiled2._engine.FindConstraint<MaxStatements>().Max);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company());

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }

        private class MyJSIndex : AbstractJavaScriptIndexCreationTask
        {
            public MyJSIndex(int? maxStepsForScript)
            {
                Maps = new HashSet<string>()
                {
                    @"
map('Companies', (company) => {
    var x = [];
    for (var i = 0; i < 50; i++) {
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

                if (maxStepsForScript.HasValue)
                {
                    Configuration = new IndexConfiguration
                    {
                        { RavenConfiguration.GetKey(x => x.Indexing.MaxStepsForScript), maxStepsForScript.ToString() }
                    };
                }
            }
        }
    }
}
