using Voron.Debugging;

namespace Raven.Server.Documents
{
    public class SlowWriteNotification
    {
        public static void Notify(CommitStats stats, DocumentDatabase database)
        {
            if (stats.NumberOf4KbsWrittenToDisk == 0 ||
                // we don't want to raise the error too often
                stats.WriteToJournalDuration.TotalMilliseconds < 500)
            {
                return;
            }

            var writtenDataInMb = stats.NumberOf4KbsWrittenToDisk / (double)256;
            var seconds = stats.WriteToJournalDuration.TotalSeconds;
            var rateOfWritesInMbPerSec = writtenDataInMb / seconds;

            if (rateOfWritesInMbPerSec < 1)
            {
                database.NotificationCenter.SlowWrites.Add(stats.JournalFilePath, writtenDataInMb, seconds);
            }
        }
    }
}
