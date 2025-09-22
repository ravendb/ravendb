using System;

namespace Voron.Impl.Journal
{
    [Flags]
    public enum TransactionMarker : byte
    {
        None = 0x0,
        Commit = 0x4
    }
}