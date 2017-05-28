using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Client.Documents
{
    public class BasicDocumentsSlow : RavenTestBase
    {
        [Fact]
        public async Task GetAsyncWithTransformer()
        {
            using (var store = GetDocumentStore())
            {
                var transformer = new Transformer();
                transformer.Execute(store);

                var dummy = JObject.FromObject(new User());
                dummy.Remove("Id");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                var requestExecuter = store
                    .GetRequestExecutor();

                using (requestExecuter.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var getDocumentCommand = new GetDocumentCommand(new[] {"users/1-A", "users/2-A" }, includes: null, transformer: transformer.TransformerName,
                        transformerParameters: null, metadataOnly: false, context: context);

                    requestExecuter
                        .Execute(getDocumentCommand, context);

                    var docs = getDocumentCommand.Result;
                    Assert.Equal(2, docs.Results.Length);

                    var doc1 = docs.Results[0] as BlittableJsonReaderObject;
                    var doc2 = docs.Results[1] as BlittableJsonReaderObject;

                    Assert.NotNull(doc1);

                    var doc1Properties = doc1.GetPropertyNames();
                    Assert.True(doc1Properties.Contains("@metadata"));
                    Assert.Equal(1 + 1, doc1Properties.Length); // +1 for @metadata

                    Assert.NotNull(doc2);

                    var doc2Properties = doc2.GetPropertyNames();
                    Assert.True(doc2Properties.Contains("@metadata"));
                    Assert.Equal(1 + 1, doc2Properties.Length); // +1 for @metadata

                    var values1 = (BlittableJsonReaderArray)doc1["$values"];
                    var values2 = (BlittableJsonReaderArray)doc2["$values"];

                    Assert.Equal(1, values1.Length);
                    Assert.Equal(1, values2.Length);

                    using (var session = (DocumentSession)store.OpenSession())
                    {
                        var user1 = (User)session.EntityToBlittable.ConvertToEntity(typeof(User), "users/1-A", (BlittableJsonReaderObject)values1[0]);
                        var user2 = (User)session.EntityToBlittable.ConvertToEntity(typeof(User), "users/2-A", (BlittableJsonReaderObject)values2[0]);

                        Assert.Equal("Fitzchak", user1.Name);
                        Assert.Equal("Arek", user2.Name);
                    }

                    getDocumentCommand = new GetDocumentCommand(new[] { "users/1-A", "users/2-A" }, includes: null, transformer: transformer.TransformerName,
                        transformerParameters: null, metadataOnly: true, context: context);

                    requestExecuter
                        .Execute(getDocumentCommand, context);

                    docs = getDocumentCommand.Result;
                    Assert.Equal(2, docs.Results.Length);

                    doc1 = docs.Results[0] as BlittableJsonReaderObject;
                    doc2 = docs.Results[1] as BlittableJsonReaderObject;

                    Assert.NotNull(doc1);

                    doc1Properties = doc1.GetPropertyNames();
                    Assert.True(doc1Properties.Contains("@metadata"));
                    Assert.Equal(1, doc1Properties.Length); // +1 for @metadata

                    Assert.NotNull(doc2);

                    doc2Properties = doc2.GetPropertyNames();
                    Assert.True(doc2Properties.Contains("@metadata"));
                    Assert.Equal(1, doc2Properties.Length); // +1 for @metadata
                }
            }
        }

        private class Transformer : AbstractTransformerCreationTask<User>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public Transformer()
            {
                TransformResults = results => from result in results
                                              select new
                                              {
                                                  result.Name
                                              };
            }
        }
    }
}