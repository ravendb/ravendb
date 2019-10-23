using System;
using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7162 : RavenTestBase
    {
        public RavenDB_7162(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void RequestTimeoutShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "John" });
                    session.SaveChanges();
                }

                using (store.SetRequestTimeout(TimeSpan.FromMilliseconds(100)))
                {
                    using (var commands = store.Commands())
                    {
                        var e = Assert.Throws<RavenException>(() => commands.Execute(new DelayCommand(TimeSpan.FromSeconds(2))));
                        Assert.Contains("failed with timeout after 00:00:00.1000000", e.ToString());
                    }
                }
            }
        }

        private class DelayCommand : RavenCommand
        {
            private readonly TimeSpan _value;

            public DelayCommand(TimeSpan value)
            {
                _value = value;
            }

            

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/test/delay?value={(int)_value.TotalMilliseconds}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }
        }
    }
}
