using System.IO;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class IncrementalBackup
    {
        private const string IncrementalBackupStateFile = "IncrementalBackup.state.json";

        public static long? ReadLastEtagsFromFile(string backupDirectory, DocumentsOperationContext context)
        {
            var etagFileLocation = Path.Combine(backupDirectory, IncrementalBackupStateFile);
            if (File.Exists(etagFileLocation) == false)
                return null;

            using (var fileStream = new FileStream(etagFileLocation, FileMode.Open))
            {
                using (var reader = context.ReadForMemory(fileStream, IncrementalBackupStateFile))
                {
                    if (reader.TryGet("LastEtag", out long lastDocsEtag) == false)
                        return null;

                    return lastDocsEtag;
                }
            }
        }
    }
}