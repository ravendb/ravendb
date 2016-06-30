using System;

namespace Sparrow.Logging
{
    [Flags]
    public enum LogMode
    {
        None = 0,
        Operations = 1,
        Information = 1 & 2
    }
}