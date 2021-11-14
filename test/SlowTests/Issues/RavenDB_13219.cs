using System;
using FastTests;
using FastTests.Server.JavaScript;
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
        [JavaScriptEngineClassData]
        public void ShouldNotBeAbleToCreateCountersWithoutNames(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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

                void testCode() 
                {
                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));
                }
                var e = (Exception)(jsEngineType == "Jint" ?  Assert.Throws<RavenException>(testCode) : Assert.Throws<JavaScriptException>(testCode));

                Assert.Contains("'name' must be a non-empty string argument", e.Message);
            }
        }
    }
}
