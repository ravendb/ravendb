using System;

namespace Raven.Server.Documents
{
    [Flags]
    public enum DocumentFlags
    {
        None = 0,
        Versioned = 1,
        Artificial = 2,
    }
}