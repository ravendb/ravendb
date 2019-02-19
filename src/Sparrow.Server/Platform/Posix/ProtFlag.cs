using System;

namespace Sparrow.Server.Platform.Posix
{
    [Flags]
    public enum ProtFlag : int
    {
        PROT_NONE = 0x0,     /* Page can not be accessed.  */
        PROT_READ = 0x1,     /* Page can be read.  */
        PROT_WRITE = 0x2,    /* Page can be written.  */
        PROT_EXEC = 0x4      /* Page can be executed.  */
    }
}
