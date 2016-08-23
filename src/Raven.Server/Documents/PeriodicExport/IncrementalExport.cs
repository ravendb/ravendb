using System.IO;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Raven.Server.Smuggler.Documents;

namespace Raven.Server.Documents.PeriodicExport
{
    public class IncrementalExport
    {
        private const string IncrementalExportStateFile = "IncrementalExport.state.json";

        public static void ReadLastEtagsFromFile(string exportDirectory, DocumentsOperationContext context, SmugglerExporter smugglerExporter)
        {
            var etagFileLocation = Path.Combine(exportDirectory, IncrementalExportStateFile);
            if (File.Exists(etagFileLocation) == false)
                return;

            using (var fileStream = new FileStream(etagFileLocation, FileMode.Open))
            {
                var reader = context.ReadForMemory(fileStream, IncrementalExportStateFile);
                reader.TryGet("LastDocsEtag", out smugglerExporter.StartDocsEtag);
            }
        }
    }
}