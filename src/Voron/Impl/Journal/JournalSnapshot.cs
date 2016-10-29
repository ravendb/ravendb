using System;
using System.Collections.Generic;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public class JournalSnapshot : IComparable<JournalSnapshot>
    {
        public long Number;
        public PageTable PageTranslationTable;
        public long AvailablePages;
        public long LastTransaction;
        public JournalFile FileInstance;

        public int CompareTo(JournalSnapshot other)
        {
            return Number.CompareTo(other.Number);
        }
    }
}
