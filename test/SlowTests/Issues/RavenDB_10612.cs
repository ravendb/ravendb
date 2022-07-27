using System;
using FastTests;
using Tests.Infrastructure;
using Raven.Client.Documents.Operations;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10612 : RavenTestBase
    {
        public RavenDB_10612(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ShouldBeAbleToPatchInDebugWhenMetadataIdPropertyIsTouched(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John"
                    }, "users/1");

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByQueryOperation("from Users update { var id = this['@metadata']['@id']; this.Name = 'Bob' }"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(15000));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Equal("Bob", user.Name);
                }
            }
        }
    }
}
