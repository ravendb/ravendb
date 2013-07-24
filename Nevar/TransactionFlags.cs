using System;

namespace Nevar
{
    [Flags]
    public enum TransactionFlags
    {
        None = 0,
        Read = 1,
        ReadWrite = 2
    }
}