using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8603 : RavenTestBase
    {
        public RavenDB_8603(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task ShouldWork(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                var input = new User
                {
                    Name = "THIS IS EVIL STRING\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nTHIS IS EVIL STRING"
                };

                var input2 = new User
                {
                    Name = "THIS IS ANOTHER EVIL STRING\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\nTHIS IS ANOTHER EVIL STRING"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(input, "users/1");
                    session.Store(input2, "users/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var output = session.Load<User>("users/1"); // this works
                    Assert.Equal(input.Name, output.Name);

                    var output2 = session.Load<User>("users/2"); // this works
                    Assert.Equal(input2.Name, output2.Name);
                }

                var operation = await store.Operations.SendAsync(new PatchByQueryOperation("from Users update { this.Test = 'test'; }"));
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = store.OpenSession())
                {
                    var output = session.Load<User>("users/1"); // should not throw
                    Assert.Equal(input.Name, output.Name);

                    var output2 = session.Load<User>("users/2"); // should not throw
                    Assert.Equal(input2.Name, output2.Name);
                }
            }
        }
    }
}
