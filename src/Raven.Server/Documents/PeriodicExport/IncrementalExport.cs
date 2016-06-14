using System.IO;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;

namespace Raven.Server.Documents.PeriodicExport
{
    public class IncrementalExport
    {
        private const string IncrementalExportStateFile = "IncrementalExport.state.json";

        public static void ReadLastEtagsFromFile(string exportDirectory, DocumentsOperationContext context, DatabaseDataExporter dataExporter)
        {
            var etagFileLocation = Path.Combine(exportDirectory, IncrementalExportStateFile);
            if (File.Exists(etagFileLocation) == false)
                return;

            using (var fileStream = new FileStream(etagFileLocation, FileMode.Open))
            {
                var reader = context.ReadForMemory(fileStream, IncrementalExportStateFile);
                reader.TryGet("LastDocsEtag", out dataExporter.StartDocsEtag);
            }
        }
    }
}