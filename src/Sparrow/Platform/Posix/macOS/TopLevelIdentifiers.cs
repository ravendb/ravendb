namespace Sparrow.Platform.Posix.macOS
{
    internal enum TopLevelIdentifiers
    {
        CTL_UNSPEC = 0,     /* unused */
        CTL_KERN = 1,       /* "high kernel": proc, limits */
        CTL_VM = 2,	        /* virtual memory */
        CTL_VFS = 3,        /* file system, mount type is next */
        CTL_NET = 4,        /* network, see socket.h */
        CTL_DEBUG = 5,      /* debugging parameters */
        CTL_HW = 6,         /* generic cpu/io */
        CTL_MACHDEP = 7,    /* machine dependent */
        CTL_USER = 8,       /* user-level */
        CTL_MAXID = 9       /* number of valid top-level ids */
    }
}
