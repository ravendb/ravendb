using System;

namespace Sparrow.Logging
{
    [Flags]
    public enum LogMode
    {
        None = 0,
        Operations = 1, // High level info for operational users
        Information = 3 // Low level debug info
    }
}