using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json.Linq;
using Raven.Client.Commands;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Sparrow.Json;
using Xunit;

namespace FastTests.Client.Documents
{
    public class BasicDocuments : RavenNewTestBase
    {
        [Fact]
        public async Task CanStoreAnonymousObject()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Fitzchak" });
                    await session.StoreAsync(new { Name = "Arek" });

                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task GetAsync()
        {
            using (var store = GetDocumentStore())
            {
                var dummy = JObject.FromObject(new User());
                dummy.Remove("Id");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                var requestExecuter = store
                    .GetRequestExecuter();

                JsonOperationContext context;
                using (requestExecuter.ContextPool.AllocateOperationContext(out context))
                {
                    var getDocumentCommand = new GetDocumentCommand
                    {
                        Ids = new[] { "users/1", "users/2" }
                    };

                    requestExecuter
                        .Execute(getDocumentCommand, context);

                    var docs = getDocumentCommand.Result;
                    Assert.Equal(2, docs.Results.Length);

                    var doc1 = docs.Results[0] as BlittableJsonReaderObject;
                    var doc2 = docs.Results[1] as BlittableJsonReaderObject;

                    Assert.NotNull(doc1);

                    var doc1Properties = doc1.GetPropertyNames();
                    Assert.True(doc1Properties.Contains("@metadata"));
                    Assert.Equal(dummy.Count + 1, doc1Properties.Length); // +1 for @metadata

                    Assert.NotNull(doc2);

                    var doc2Properties = doc2.GetPropertyNames();
                    Assert.True(doc2Properties.Contains("@metadata"));
                    Assert.Equal(dummy.Count + 1, doc2Properties.Length); // +1 for @metadata

                    using (var session = (DocumentSession)store.OpenSession())
                    {
                        var user1 = (User)session.EntityToBlittable.ConvertToEntity(typeof(User), "users/1", doc1);
                        var user2 = (User)session.EntityToBlittable.ConvertToEntity(typeof(User), "users/2", doc2);

                        Assert.Equal("Fitzchak", user1.Name);
                        Assert.Equal("Arek", user2.Name);
                    }

                    getDocumentCommand = new GetDocumentCommand
                    {
                        Ids = new[] { "users/1", "users/2" },
                        MetadataOnly = true
                    };

                    requestExecuter
                        .Execute(getDocumentCommand, context);

                    docs = getDocumentCommand.Result;
                    Assert.Equal(2, docs.Results.Length);

                    doc1 = docs.Results[0] as BlittableJsonReaderObject;
                    doc2 = docs.Results[1] as BlittableJsonReaderObject;

                    Assert.NotNull(doc1);

                    doc1Properties = doc1.GetPropertyNames();
                    Assert.True(doc1Properties.Contains("@metadata"));
                    Assert.Equal(1, doc1Properties.Length);

                    Assert.NotNull(doc2);

                    doc2Properties = doc2.GetPropertyNames();
                    Assert.True(doc2Properties.Contains("@metadata"));
                    Assert.Equal(1, doc2Properties.Length);
                }
            }
        }

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
                    .GetRequestExecuter();

                JsonOperationContext context;
                using (requestExecuter.ContextPool.AllocateOperationContext(out context))
                {
                    var getDocumentCommand = new GetDocumentCommand
                    {
                        Ids = new[] { "users/1", "users/2" },
                        Transformer = transformer.TransformerName
                    };

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
                        var user1 = (User)session.EntityToBlittable.ConvertToEntity(typeof(User), "users/1", (BlittableJsonReaderObject)values1[0]);
                        var user2 = (User)session.EntityToBlittable.ConvertToEntity(typeof(User), "users/2", (BlittableJsonReaderObject)values2[0]);

                        Assert.Equal("Fitzchak", user1.Name);
                        Assert.Equal("Arek", user2.Name);
                    }

                    getDocumentCommand = new GetDocumentCommand
                    {
                        Ids = new[] { "users/1", "users/2" },
                        Transformer = transformer.TransformerName,
                        MetadataOnly = true
                    };

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