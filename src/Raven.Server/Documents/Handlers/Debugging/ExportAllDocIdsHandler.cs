using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class AllDocumentIdsDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/export-all-ids", "GET")]
        public Task ExportAllDocIds()
        {
            var lastFlush = 0;
            var path = GetStringQueryString("path", required: true);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var fileStream = new FileStream(path, FileMode.Create))
            using (context.OpenReadTransaction())
            {
                foreach (var id in context.DocumentDatabase.DocumentsStorage.GetAllIds(context))
                {
                    var pos = WriteLine(fileStream, $"{id}{Environment.NewLine}");
                    if (lastFlush - pos > 4096)
                    {
                        fileStream.Flush();
                        lastFlush = pos;
                    }
                }
                fileStream.Flush();
            }
            return Task.CompletedTask;
        }

        private static int WriteLine(FileStream fileStream, string str)
        {
            var bytes = Encoding.UTF8.GetBytes(str);
            fileStream.Write(bytes, 0, bytes.Length);
            return bytes.Length;
        }
    }
} 