using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class AllDocumentIdsDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/export-all-ids", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task ExportAllDocIds()
        {
            var fileName = $"ids-for-{Uri.EscapeDataString(Database.Name)}-{Database.Time.GetUtcNow().GetDefaultRavenFormat(isUtc: true)}.txt";
            HttpContext.Response.Headers["Content-Disposition"] = $"attachment; filename={fileName}";

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new StreamWriter(ResponseBodyStream(), Encoding.UTF8, 4096))
            using (context.OpenReadTransaction())
            {
                foreach (var id in context.DocumentDatabase.DocumentsStorage.GetAllIds(context))
                    writer.Write($"{id}{Environment.NewLine}");

                writer.Flush();
            }

            return Task.CompletedTask;
        }
    }
}
