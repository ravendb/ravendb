using System;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12775 : RavenTestBase
    {
        public RavenDB_12775(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task Can_patch_expires_in_metadata(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                const string queryString = @"from Users 
                update {
                    var timestamp = 1548758640;
                    var date = new Date(timestamp);
                    this['@metadata']['@expires'] = date;
                }";

                var op = await store.Operations.SendAsync(new PatchByQueryOperation(queryString));

                await op.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.Expires, out var expires));
                    Assert.Equal("1970-01-18T22:12:38.6400000Z", expires);
                }
            }
        }


        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task Test_patch_should_return_original_doc_if_patch_status_is_not_modified(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                const string queryString = @"this['@metadata']['@foo'] = 'foo';";

                var rq = store.GetRequestExecutor();

                using (rq.ContextPool.AllocateOperationContext(out var ctx))
                {
                    var cmd = new PatchOperation.PatchCommand(ctx, "users/1", changeVector:null,
                        new PatchRequest
                        {
                            Script = queryString
                        }, patchIfMissing:null, skipPatchIfChangeVectorMismatch:false, returnDebugInformation:false, 
                        test: true);

                    await rq.ExecuteAsync(cmd, ctx);

                    PatchResult commandResult = cmd.Result;

                    Assert.True(commandResult.Status == PatchStatus.NotModified);

                    var metadata = commandResult.ModifiedDocument.GetMetadata();
                    Assert.NotNull(metadata);

                    Assert.False(metadata.TryGet("@foo", out object _));

                }
            }
        }

    }
}
