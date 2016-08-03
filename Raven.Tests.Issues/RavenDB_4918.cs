using System;
using System.Linq;
using System.Net.Http;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4918 : RavenTestBase
    {
        [Fact]
        public void CanGenerateCSharpDefintionForMultiMap()
        {
            using (var documentStore = NewRemoteDocumentStore())
            {
                documentStore.ExecuteIndex(new MultiMap());

                var request = documentStore.JsonRequestFactory.CreateHttpJsonRequest(
                    new CreateHttpJsonRequestParams(null, documentStore.Url.ForDatabase(documentStore.DefaultDatabase) + "/c-sharp-index-definition/MultiMap", HttpMethod.Get,
                        documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));

                var response = request.ReadResponseJson().ToString();

                Assert.Contains("from order in docs.Collection1", response);
                Assert.Contains("from order in docs.Collection2", response);
            }
        }


        public class MultiMap : AbstractIndexCreationTask
        {
            public override string IndexName => "MultiMap";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =  {
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