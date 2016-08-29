//-----------------------------------------------------------------------
// <copyright file="ComplexIndexOnNotAnalyzedField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class ComplexIndexOnNotAnalyzedField : RavenTestBase
    {
        [Fact]
        public void CanQueryOnKey()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put("companies/", null,
                       RavenJObject.Parse("{'Name':'Hibernating Rhinos', 'Partners': ['companies/49', 'companies/50']}"),
                       RavenJObject.Parse("{'Raven-Entity-Name': 'Companies'}"));


                store.DatabaseCommands.PutIndex("CompaniesByPartners", new IndexDefinition
                {
                    Maps = { "from company in docs.Companies from partner in company.Partners select new { Partner = partner }" }
                });

                QueryResult queryResult;
                do
                {
                    queryResult = store.DatabaseCommands.Query("CompaniesByPartners", new IndexQuery
                    {
                        Query = "Partner:companies/49",
                        PageSize = 10
                    });
                } while (queryResult.IsStale);

                Assert.Equal("Hibernating Rhinos", queryResult.Results[0].Value<string>("Name"));
            }
        }
    }
}
