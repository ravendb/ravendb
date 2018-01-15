using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class GetRevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/get-revisions", "GET", AuthorizationStatus.ValidUser)]
        public Task GetRevisions()
        {
            long frometag = 0;
            var frometagStr = GetLongQueryString("from-etag", false);
            if (frometagStr != null)
                frometag = Convert.ToInt64(frometagStr);
            
            var fileName = $"revisions-for-{Uri.EscapeDataString(Database.Name)}-{Database.Time.GetUtcNow().GetDefaultRavenFormat(isUtc: true)}.txt";
            HttpContext.Response.Headers["Content-Disposition"] = $"attachment; filename={fileName}";

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new StreamWriter(ResponseBodyStream(), Encoding.UTF8, 4096))
            using (context.OpenReadTransaction())
            {

                writer.Write($"get-revisions from etag={frometag}");
                writer.Flush();
                
                var revisionsStorage = Database.DocumentsStorage.RevisionsStorage;
                var revisions = revisionsStorage.GetRevisionsFrom(context, frometag, int.MaxValue);

                var docCount = 0;
                foreach (var id in revisions)
                {
                    writer.Write($"Id={id.Id}{Environment.NewLine}");
                    writer.Write($"Etag={id.Etag}{Environment.NewLine}");
                    writer.Write($"LastModified={id.LastModified}{Environment.NewLine}");
                    writer.Write($"StorageId={id.StorageId}{Environment.NewLine}");
                    writer.Write($"ChangeVector={id.ChangeVector}{Environment.NewLine}");

                    if (docCount++ % 100 == 0)
                        writer.Flush();
                }
                writer.Flush();
            }

            return Task.CompletedTask;
        }
    }
}
