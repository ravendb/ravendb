//-----------------------------------------------------------------------
// <copyright file="ComplexIndexOnNotAnalyzedField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Text;
using FastTests;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class ComplexIndexOnNotAnalyzedField : RavenNewTestBase
    {
        [Fact]
        public void CanQueryOnKey()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("{'Name':'Hibernating Rhinos', 'Partners': ['companies/49', 'companies/50']}")))
                    {
                        var json = commands.Context.ReadForMemory(stream, "doc");
                        commands.Put("companies/", null, json, new Dictionary<string, string> { { Constants.Metadata.Collection, "Companies" } });
                    }

                    store.Admin.Send(new PutIndexOperation("CompaniesByPartners", new IndexDefinition
                    {
                        Maps = { "from company in docs.Companies from partner in company.Partners select new { Partner = partner }" }
                    }));

                    QueryResult queryResult;
                    do
                    {
                        queryResult = commands.Query("CompaniesByPartners", new IndexQuery(store.Conventions)
                        {
                            Query = "Partner:companies/49",
                            PageSize = 10
                        });
                    } while (queryResult.IsStale);

                    var result = (BlittableJsonReaderObject)queryResult.Results[0];
                    string name;
                    Assert.True(result.TryGet("Name", out name));
                    Assert.Equal("Hibernating Rhinos", name);
                }
            }
        }
    }
}
