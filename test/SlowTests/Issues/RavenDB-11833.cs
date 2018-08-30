using System.Collections.Generic;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11833 : RavenTestBase
    {

        [Fact]
        public void PatchOnCountersShouldThrowWhenFeaturesAvailabilityIsSetToStable()
        {

            using (var store = GetDocumentStore(new Options
            {
                Server  = GetNewServer(new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability), "Stable"}
                })
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order(), "orders/1");
                    session.SaveChanges();
                }

                var e = Assert.Throws<RavenException>(() => 
                    store.Operations.Send(new PatchOperation(
                        "orders/1", 
                        null, 
                        new PatchRequest
                        {
                            Script = "incrementCounter(this, 'likes', 100)"
                        })));

                Assert.Contains("Raven.Server.Exceptions.FeaturesAvailabilityException: Can not use 'Counters', " +
                                "as this is an experimental feature and the server does not support experimental features.", e.Message);

                e = Assert.Throws<RavenException>(() =>
                    store.Operations.Send(new PatchOperation(
                        "orders/1",
                        null,
                        new PatchRequest
                        {
                            Script = "deleteCounter(this, 'likes')"
                        })));

                Assert.Contains("Raven.Server.Exceptions.FeaturesAvailabilityException: Can not use 'Counters', " +
                                "as this is an experimental feature and the server does not support experimental features.", e.Message);
            }
        }

    }
}
