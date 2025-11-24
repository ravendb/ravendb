using System;

namespace Voron
{
    [Flags]
    public enum TransactionPersistenceModeFlags
    {
        Encrypted = 1,
        LinkedJournalsRecord = 16,
        HasFreePages = 32
    }
}
