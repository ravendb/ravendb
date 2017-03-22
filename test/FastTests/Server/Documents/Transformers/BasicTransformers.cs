using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Raven.Client.Exceptions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server;
using Raven.Server.Documents.Transformers;
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
            using (var store = GetDocumentStore(modifyName: x => "CanPersistDB", defaultServer: server, deleteDatabaseWhenDisposed: false, modifyDatabaseDocument: x => x.Settings["Raven/RunInMemory"] = "False"))
            {
                var task1 = store.Admin.SendAsync(new PutTransformerOperation(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Name })",
                    LockMode = TransformerLockMode.LockedIgnore,
                    Temporary = true,
                    Name = "Transformer1"
                }));

                var task2 = store.Admin.SendAsync(new PutTransformerOperation(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Email })",
                    LockMode = TransformerLockMode.Unlock,
                    Temporary = false,
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
                Assert.True(transformer.Definition.Temporary);

                transformer = transformers[1];
                Assert.Equal("Transformer2", transformer.Name);
                Assert.Equal("Transformer2", transformer.Definition.Name);
                Assert.Equal("results.Select(x => new { Name = x.Email })", transformer.Definition.TransformResults);
                Assert.Equal(TransformerLockMode.Unlock, transformer.Definition.LockMode);
                Assert.False(transformer.Definition.Temporary);
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
                    Temporary = true,
                    Name = "Transformer1"
                }));

                TransactionOperationContext context;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    context.OpenReadTransaction();

                    var databaseRecord = Server.ServerStore.Cluster.ReadDatabase(context, store.DefaultDatabase);
                    databaseRecord.Transformers["Transformer1"].TransformResults = "yellow world";

                    var blittableJsonReaderObject = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                    var index = await Server.ServerStore.TEMP_WriteDbAsync(context, store.DefaultDatabase, blittableJsonReaderObject, null);
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

        [Fact(Skip = "Maxim:Investigate")]
        public async Task CanDelete()
        {
            using (var server = GetNewServer(deletePrevious: true))
            using (GetDocumentStore(modifyName: x => "CanDelete", defaultServer: server))
            {

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("CanDelete");
                database.TransformerStore.CreateTransformer(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Name })",
                    LockMode = TransformerLockMode.LockedIgnore,
                    Temporary = true,
                    Name = "Transformer1"
                });

                var encodedName = Convert.ToBase64String(Encoding.UTF8.GetBytes("Transformer1"));

                Assert.Equal(1, database.TransformerStore.GetTransformers().Count());

                database.TransformerStore.DeleteTransformer("Transformer1");

                Assert.Equal(0, database.TransformerStore.GetTransformers().Count());
            }
        }
    }
}