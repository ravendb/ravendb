﻿using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4918 : RavenTestBase
    {
        public RavenDB_4918(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGenerateCSharpDefintionForMultiMap()
        {
            using (var documentStore = GetDocumentStore())
            {
                await documentStore.ExecuteIndexAsync(new MultiMap());

                using (var client = new HttpClient().WithConventions(documentStore.Conventions))
                {
                    var url = $"{documentStore.Urls.First()}/databases/{documentStore.Database}/indexes/c-sharp-index-definition?name=MultiMap";
                    var response = await client.GetStringAsync(url);

                    Assert.Contains("from order in docs.Collection1", response);
                    Assert.Contains("from order in docs.Collection2", response);
                }
            }
        }

        private class MultiMap : AbstractIndexCreationTask
        {
            public override string IndexName => "MultiMap";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from order in docs.Collection1
select new { order.Company }",
                        @"from order in docs.Collection2
select new { order.Company }"
                    }
                };
            }
        }
    }
}
