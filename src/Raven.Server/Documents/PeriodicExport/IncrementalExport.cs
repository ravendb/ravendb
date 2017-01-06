using System.IO;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.PeriodicExport
{
    public class IncrementalExport
    {
        private const string IncrementalExportStateFile = "IncrementalExport.state.json";

        public static long? ReadLastEtagsFromFile(string exportDirectory, DocumentsOperationContext context)
        {
            var etagFileLocation = Path.Combine(exportDirectory, IncrementalExportStateFile);
            if (File.Exists(etagFileLocation) == false)
                return null;

            using (var fileStream = new FileStream(etagFileLocation, FileMode.Open))
            {
                using (var reader = context.ReadForMemory(fileStream, IncrementalExportStateFile))
                {
                    long lastDocsEtag;
                    if (reader.TryGet("LastDocsEtag", out lastDocsEtag) == false)
                        return null;

                    return lastDocsEtag;
                }
            }
        }
    }
}