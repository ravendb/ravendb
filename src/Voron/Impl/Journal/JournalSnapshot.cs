using System;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public class JournalSnapshot : IComparable<JournalSnapshot>
    {
        public long Number;
        public PageTable PageTranslationTable;
        public long Available4Kbs;
        public long LastTransaction;
        public JournalFile FileInstance;
        public long WritePosIn4KbPosition;

        public int CompareTo(JournalSnapshot other)
        {
            return Number.CompareTo(other.Number);
        }
    }
}
