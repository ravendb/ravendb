using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace FastTests.Server.Documents.Transformers
{
    public class BasicTransformers : RavenTestBase
    {
        [Fact]
        public async Task CanPersist()
        {
            using (var server = GetNewServer(runInMemory: false, partialPath: "CanPersist"))
            using (var store = GetDocumentStore(modifyName: x => "CanPersistDB", defaultServer: server, deleteDatabaseWhenDisposed: false, modifyDatabaseRecord: x => x.Settings["Raven/RunInMemory"] = "False"))
            {
                var task1 = store.Admin.SendAsync(new PutTransformerOperation(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Name })",
                    LockMode = TransformerLockMode.LockedIgnore,
                    Name = "Transformer1"
                }));

                var task2 = store.Admin.SendAsync(new PutTransformerOperation(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Email })",
                    LockMode = TransformerLockMode.Unlock,
                    Name = "Transformer2"
                }));

                await Task.WhenAll(task1, task2);
            }

            using (var server = GetNewServer(runInMemory: false, deletePrevious: false, partialPath: "CanPersist"))
            {
                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("CanPersistDB");

                var transformers = database
                    .TransformerStore
                    .GetTransformers()
                    .OrderBy(x => x.Name)
                    .ToList();

                Assert.Equal(2, transformers.Count);

                var transformer = transformers[0];
                Assert.Equal("Transformer1", transformer.Name);
                Assert.Equal("Transformer1", transformer.Definition.Name);
                Assert.Equal("results.Select(x => new { Name = x.Name })", transformer.Definition.TransformResults);
                Assert.Equal(TransformerLockMode.LockedIgnore, transformer.Definition.LockMode);

                transformer = transformers[1];
                Assert.Equal("Transformer2", transformer.Name);
                Assert.Equal("Transformer2", transformer.Definition.Name);
                Assert.Equal("results.Select(x => new { Name = x.Email })", transformer.Definition.TransformResults);
                Assert.Equal(TransformerLockMode.Unlock, transformer.Definition.LockMode);
            }
        }


        [Fact]
        public async Task WillLoadAsFaulty()
        {
            using (var store = GetDocumentStore())
            {
                await store.Admin.SendAsync(new PutTransformerOperation(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Name })",
                    LockMode = TransformerLockMode.LockedIgnore,
                    Name = "Transformer1"
                }));

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    context.OpenReadTransaction();

                    var databaseRecord = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                    databaseRecord.Transformers["Transformer1"].TransformResults = "yellow world";

                    var blittableJsonReaderObject = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                    var (index, _) = await Server.ServerStore.WriteDbAsync(store.Database, blittableJsonReaderObject, null);
                    await Server.ServerStore.Cluster.WaitForIndexNotification(index);
                }

                var database = await GetDocumentDatabaseInstanceFor(store);

                var transformers = database
                    .TransformerStore
                    .GetTransformers()
                    .OrderBy(x => x.Name)
                    .ToList();

                Assert.Equal
                (
                    1,
                    transformers.Count
                );

                var transformer = transformers[0];
                Assert.Equal
                (
                    "Transformer1",
                    transformer.Name
                );

                Assert.Throws<RavenException>(() => store.Admin.Send(new SetTransformerLockOperation("Transformer1", TransformerLockMode.LockedIgnore)));

            }
        }

        [Fact]
        public async Task CanDelete()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TransformerStore.CreateTransformer(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Name })",
                    LockMode = TransformerLockMode.LockedIgnore,
                    Name = "Transformer1"
                });

                Assert.Equal(1, database.TransformerStore.GetTransformers().Count());

                await database.TransformerStore.DeleteTransformer("Transformer1");

                Assert.Equal(0, database.TransformerStore.GetTransformers().Count());
            }
        }
    }
}