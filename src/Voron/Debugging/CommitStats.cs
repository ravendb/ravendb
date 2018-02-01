using System;
using System.Diagnostics;

namespace Voron.Debugging
{
    public class CommitStats
    {
        public TimeSpan WriteToJournalDuration;

        public int NumberOfModifiedPages;

        public int NumberOf4KbsWrittenToDisk;
    }
}
