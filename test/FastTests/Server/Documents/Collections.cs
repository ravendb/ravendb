using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server.Documents;
using Xunit;

namespace FastTests.Server.Documents
{
    public class Collections : RavenTestBase
    {
        [Fact]
        public void CanSurviveRestart()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {
                store.DatabaseCommands.Put("orders/1", null, new RavenJObject(), new RavenJObject
                {
                    {
                        Constants.Headers.RavenEntityName, "Orders"
                    }
                });

                store.DatabaseCommands.Put("orders/2", null, new RavenJObject(), new RavenJObject
                {
                    {
                        Constants.Headers.RavenEntityName, "orders"
                    }
                });

                store.DatabaseCommands.Put("people/1", null, new RavenJObject(), new RavenJObject
                {
                    {
                        Constants.Headers.RavenEntityName, "People"
                    }
                });

                var collections = GetAllCollections(store);

                Assert.Equal(2, collections.Count);

                var orders = collections.First(x => x.Name == "Orders");
                Assert.Equal(2, orders.Count);

                var people = collections.First(x => x.Name == "People");
                Assert.Equal(1, people.Count);
            }

            using (var store = GetDocumentStore(path: path))
            {
                var collections = GetAllCollections(store);

                Assert.Equal(2, collections.Count);

                var orders = collections.First(x => x.Name == "Orders");
                Assert.Equal(2, orders.Count);

                var people = collections.First(x => x.Name == "People");
                Assert.Equal(1, people.Count);
            }
        }

        private static List<DocumentsStorage.CollectionStat> GetAllCollections(DocumentStore store)
        {
            var request = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store.Url.ForDatabase(store.DefaultDatabase) + "/collections/stats", HttpMethod.Get, store.DatabaseCommands.PrimaryCredentials, store.Conventions));
            var json = (RavenJObject)request.ReadResponseJson();
            var collections = json.Value<RavenJObject>("Collections");

            var results = new List<DocumentsStorage.CollectionStat>();
            foreach (var key in collections.Keys)
            {
                results.Add(new DocumentsStorage.CollectionStat
                {
                    Name = key,
                    Count = collections.Value<int>(key)
                });
            }

            return results;
        }
    }
}