using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17196 : RavenTestBase
{
    public RavenDB_17196(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Patching)]
    public async Task can_patch_metadata()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestDocument { Name = "TestDocument", Id = "test/1" });
                await session.SaveChangesAsync();
            }

            // New Sesson so document is not in cache
            using (var session = store.OpenAsyncSession())
            {
                var metadataPatch = new PatchRequest
                {
                    Script = @"
                                    this.my_first_value = args.myval_1;
                                    this.my_second_value = args.myval_2; 
                                    this['@metadata']['my_first_value_metadata'] = args.myval_1;
                                    this['@metadata']['my_second_value_metadata'] = args.myval_2;
                                   ",
                    Values = {
                            { "myval_1", "this does not work" },
                            { "myval_2", "but this does!" }
                        }
                };
                session.Advanced.Defer(new PatchCommandData("test/1", null, metadataPatch, null));
                await session.SaveChangesAsync();
            }

            // New Sesson so document is not in cache
            using (var session = store.OpenAsyncSession())
            {
                var document = await session.LoadAsync<TestDocument2>("test/1");
                var metadata = session.Advanced.GetMetadataFor(document);
                Assert.Equal(document.my_first_value, "this does not work");
                Assert.Equal(document.my_second_value, "but this does!");
                Assert.True(metadata.ContainsKey("my_first_value_metadata"), "'my_second_value' exists");   // this is true
                Assert.True(metadata.ContainsKey("my_second_value_metadata"), "'my_first_value' exists");    // this failes
            }
        }
    }

    [RavenFact(RavenTestCategory.Patching)]
    public async Task can_patch_metadata_2()
    {
        using (var store = GetDocumentStore())
        {
            var test = new TestDocument();
            const string id = "test/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(test, id);
                await session.SaveChangesAsync();
            }

            Dictionary<string, object> values = new()
            {
                {
                    "processed", "yes"
                },
                {
                    "verify", true
                }
            };

            var patchRequest = new PatchRequest
            {
                Script = "this['@metadata']['processed'] = args.processed; this['@metadata']['verify'] = args.verify;",
                Values = values
            };

            store.Operations.Send(new PatchOperation(id, null, patchRequest, patchIfMissing: null));

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<TestDocument>(test.Id);
                var m = session.Advanced.GetMetadataFor(loaded);

                m.TryGetValue("processed", out object processed);
                m.TryGetValue("verify", out object verify);

                Assert.Equal(values["processed"], processed);
                Assert.Equal(values["verify"], verify);
            }
        }
    }

    private class TestDocument
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class TestDocument2
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string my_first_value { get; set; }
        public string my_second_value { get; set; }
    }
}
