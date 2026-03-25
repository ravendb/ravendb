using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25356 : RavenTestBase
    {
        public RavenDB_25356(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Run_RawQuery_With_OptionalChaining_And_TemplateLiteral(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var rql = @"from ""Employees""  as e
select {
    F: e?.FirstName,
    Name: `${e.FirstName} ${e.LastName}`,
}";

                    // The goal of the test is just to ensure this runs without any errors
                    // Materialize the results to force server-side execution
                    var results = session.Advanced.RawQuery<object>(rql)
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Empty(results);
                }
            }
        }

        private class Companies_ByNamePhoneCity_JS : AbstractJavaScriptIndexCreationTask
        {
            public Companies_ByNamePhoneCity_JS()
            {
                Maps = new HashSet<string>
                {
                    @"map(""Companies"", (company) => {
    if (company.Address.Country === ""USA"") {
        return {
            Name: company?.Name,
            Phone: `{company.Extension}-{company.Phone}`,
            City: company.Address.City
        };
    }
})"
                };
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Create_JavaScript_Index_With_OptionalChaining_And_TemplateLiteral(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Companies_ByNamePhoneCity_JS().Execute(store);
            }
        }
    }
}
