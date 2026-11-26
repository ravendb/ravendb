using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Server.Routing;
using Raven.Server.Storage;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class AdminStorageIoHandler : ServerRequestHandler
    {
        // device level state is shared by every database on the disk, so it is Operator only
        [RavenAction("/admin/debug/storage/io", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task StorageIo()
        {
            var report = StorageIoReport.Create(ServerStore.StorageDeviceMonitor);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                using (var json = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(report, context))
                    writer.WriteObject(json);
            }
        }
    }
}
