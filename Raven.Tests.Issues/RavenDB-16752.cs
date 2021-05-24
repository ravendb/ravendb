using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_16752 : RavenTestBase
    {
        [Fact]
        public void Can_Transform_A_Dollar_Sign()
        {
            using (var store = NewDocumentStore())
            {
                new Transformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Message
                    {
                        Headers = new Dictionary<string, string>
                        {
                            {"NServiceBus.OriginatingEndpoint", "PurchaseOrderService.1.0"},
                            {"$.diagnostics.originating.hostid", "4f8138bdb0421ffe1ceaee86e9145721"},
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<Message>()
                        .TransformWith<Transformer, Transformer.Output>()
                        .ToList();

                    Assert.Equal(2, result[0].Headers.Count());
                }
            }
        }

        public class Message
        {
            public string Id { get; set; }

            public Dictionary<string, string> Headers { get; set; }
        }

        public class Transformer : AbstractTransformerCreationTask<Transformer.Output>
        {
            public Transformer()
            {
                TransformResults = messages => from message in messages
                                               let headers = message.Headers
                                               select new
                                               {
                                                   //the reason the we need to use a KeyValuePair<string, object> is that raven seems to interpret the values and convert them
                                                   // to real types. In this case it was the NServiceBus.Temporary.DelayDeliveryWith header to was converted to a timespan
                                                   Headers = headers.Select(header => new KeyValuePair<string, object>(header.Key, header.Value)),
                                               };
            }

            public class Output
            {
                public IEnumerable<KeyValuePair<string, object>> Headers { get; set; }
            }
        }
    }
}
