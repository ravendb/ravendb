using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.SampleData
{
    internal sealed class SampleDataHandlerProcessorForPostSampleData : AbstractSampleDataHandlerProcessorForPostSampleData<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SampleDataHandlerProcessorForPostSampleData([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ExecuteSmugglerAsync(JsonOperationContext context, Stream sampleDataStream, DatabaseItemType operateOnTypes)
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

            using (var source = new StreamSource(sampleDataStream, context, RequestHandler.DatabaseName))
            {
                var destination = RequestHandler.Database.Smuggler.CreateDestination();
                var options = new DatabaseSmugglerOptionsServerSide
                {
                    OperateOnTypes = operateOnTypes,
                    SkipRevisionCreation = true

                };
                options.AuthorizationStatus = AuthorizationStatus.ValidUser;
                if (feature != null )
                {
                    if (feature.AuthorizedDatabases.TryGetValue(RequestHandler.DatabaseName, out var databaseAccess) && databaseAccess == DatabaseAccess.Admin)
                        options.AuthorizationStatus = AuthorizationStatus.DatabaseAdmin;
                    if (feature.Status == RavenServer.AuthenticationStatus.ClusterAdmin)
                        options.AuthorizationStatus = AuthorizationStatus.ClusterAdmin;
                }
                var smuggler = RequestHandler.Database.Smuggler.Create(source, destination, context, options);
                await smuggler.ExecuteAsync();
            }
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
