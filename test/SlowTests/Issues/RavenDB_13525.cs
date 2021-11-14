using System;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.Dynamic;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13525 : RavenTestBase
    {
        public RavenDB_13525(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Patch_by_id_must_not_cause_endless_operation(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User()
                            {
                                Name = "abc",
                                Count = i
                            }, "users/" + i);
                    }

                    session.SaveChanges();
                }

                // if patch is actually no-op then we have a problem that the operation never completes

                store.Operations.Send(new PatchByQueryOperation(@"from Users where id() = 'users/3' update 
                {
                    if (this.Name == 'different name') // always FALSE to reproduce the issue
                        this.NewProp = 'a'
                }"))
                    .WaitForCompletion(TimeSpan.FromSeconds(30));

                store.Operations.Send(new PatchByQueryOperation(@"from Users where startsWith(id(), 'users/3') update
                {
                    this.Name = 'abc' // intentionally leaving the same value so no document will be patched actually
                }"))
                    .WaitForCompletion(TimeSpan.FromSeconds(30));
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void Patch_by_starts_with_and_id_must_not_cause_endless_operation(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < CollectionRunner.OperationBatchSize + 200; i++)
                    {
                        session.Store(new User()
                            {
                                Name = "abc",
                                Count = i
                            }, "users/" + i);
                    }

                    session.SaveChanges();
                }

                // if patch is actually no-op then we have a problem that the operation never completes

                store.Operations.Send(new PatchByQueryOperation(@"from Users where startsWith(id(), 'users/') update
                {
                    this.Name = 'abc' // intentionally leaving the same value so no document will be patched actually
                }"))
                    .WaitForCompletion(TimeSpan.FromSeconds(30));
            }
        }
    }
}
