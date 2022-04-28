using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.SampleData
{
    internal class SampleDataHandlerProcessorForPostSampleData : AbstractSampleDataHandlerProcessorForPostSampleData<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SampleDataHandlerProcessorForPostSampleData([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override async ValueTask ExecuteSmugglerAsync(JsonOperationContext context, ISmugglerSource source, Stream sampleData, DatabaseItemType operateOnTypes)
        {
            var destination = new DatabaseDestination(RequestHandler.Database);

            var smuggler = SmugglerBase.GetDatabaseSmuggler(RequestHandler.Database, source, destination, RequestHandler.Database.Time, context,
                options: new DatabaseSmugglerOptionsServerSide
                {
                    OperateOnTypes = operateOnTypes,
                    SkipRevisionCreation = true
                });

            await smuggler.ExecuteAsync();
        }

        protected override ValueTask<bool> IsDatabaseEmptyAsync()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var collection in RequestHandler.Database.DocumentsStorage.GetCollections(context))
                {
                    if (collection.Count > 0)
                    {
                        return ValueTask.FromResult(false);
                    }
                }
            }
            return ValueTask.FromResult(true);
        }
    }
}
