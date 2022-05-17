using Tests.Infrastructure;
using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16975 : RavenTestBase
    {
        public RavenDB_16975(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task Should_Not_Send_Include_Message()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Arava"
                    }, "people/1");
                    
                    session.SaveChanges();
                }
                
                var id = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = @"from People"
                });
                await using (var sub = store.Subscriptions.GetSubscriptionWorker<Person>(id))
                {
                    var mre = new AsyncManualResetEvent();
                    var r = sub.Run(batch =>
                    {
                        Assert.NotEmpty(batch.Items);
                        using (var s = batch.OpenSession())
                        {
                            Assert.Equal(0, batch.NumberOfIncludes);
                            Assert.Equal(0, s.Advanced.NumberOfRequests);
                        }
                        mre.Set();
                    });
                    
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(60)));
                    await sub.DisposeAsync();
                    await r;// no error
                }

            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task Should_Not_Send_Include_Message_Via_JavaScript(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Arava"
                    }, "people/1");
                    
                    session.SaveChanges();
                }
                var id = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = @"declare function f(p) { 
    return p;
}
from People as people
select f(people)
"
                });

                await using (var sub = store.Subscriptions.GetSubscriptionWorker<Person>(id))
                {
                    var mre = new AsyncManualResetEvent();
                    var r = sub.Run(batch =>
                    {
                        Assert.NotEmpty(batch.Items);
                        using (var s = batch.OpenSession())
                        {
                            Assert.Equal(0, batch.NumberOfIncludes);
                            Assert.Equal(0, s.Advanced.NumberOfRequests);
                        }
                        mre.Set();
                    });
                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(60)));
                    await sub.DisposeAsync();
                    await r;// no error
                }

            }
        }
    }
}
