using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Xunit;

namespace FastTests.Server.Documents
{
    public class Collections : RavenTestBase
    {
        [Fact]
        public void CanSurviveRestart()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                using (var commands = store.Commands())
                {
                    commands.Put("orders/1", null, new { }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Orders"}
                    });

                    commands.Put("orders/2", null, new { }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "orders"}
                    });

                    commands.Put("people/1", null, new { }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "People"}
                    });

                    var collectionStats = store.Admin.Send(new GetCollectionStatisticsOperation());

                    Assert.Equal(2, collectionStats.Collections.Count);

                    var orders = collectionStats.Collections.First(x => x.Key == "Orders");
                    Assert.Equal(2, orders.Value);

                    var people = collectionStats.Collections.First(x => x.Key == "People");
                    Assert.Equal(1, people.Value);
                }
            }

            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                var collectionStats = store.Admin.Send(new GetCollectionStatisticsOperation());

                Assert.Equal(2, collectionStats.Collections.Count);

                var orders = collectionStats.Collections.First(x => x.Key == "Orders");
                Assert.Equal(2, orders.Value);

                var people = collectionStats.Collections.First(x => x.Key == "People");
                Assert.Equal(1, people.Value);
            }
        }
    }
}
