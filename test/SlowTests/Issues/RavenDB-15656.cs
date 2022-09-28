using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15656 : RavenTestBase
    {
        public RavenDB_15656(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCreateMapIndexWithMissingExpressionInFor(Options options)
        {
            using DocumentStore s = GetDocumentStore(options);
            new MyIndex().Execute(s);
        }

        public class MyIndex : AbstractJavaScriptIndexCreationTask
        {
            public MyIndex()
            {
                Maps =new HashSet<string>
                {
                    @"map('Companies', (company) => {
                    return {
                        City: test()
                    };

                    function test() {
                        for (let x = 0;; x++) {}
                    }
                })"};
            }
        }
    }
}
