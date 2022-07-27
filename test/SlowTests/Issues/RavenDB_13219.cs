using System;
using FastTests;
using Tests.Infrastructure;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Patching;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13219 : RavenTestBase
    {
        public RavenDB_13219(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ShouldNotBeAbleToCreateCountersWithoutNames(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByQueryOperation(@"from Companies 
                update {
                    incrementCounter(id(this), '', 5)
                }"));

                var e = (Exception)Assert.Throws<RavenException>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(15)));

                Assert.Contains("'name' must be a non-empty string argument", e.Message);
            }
        }
    }
}
