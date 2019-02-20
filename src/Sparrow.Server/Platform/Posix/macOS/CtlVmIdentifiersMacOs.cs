namespace Sparrow.Platform.Posix.macOS
{
    internal enum CtlVmIdentifiers
    {
        VM_METER = 1,		/* struct vmmeter */
        VM_LOADAVG = 2,	    /* struct loadavg */
        /*
         * Note: "3" was skipped sometime ago and should probably remain unused
         * to avoid any new entry from being accepted by older kernels...
         */
        VM_MACHFACTOR = 4,	/* struct loadavg with mach factor*/
        VM_SWAPUSAGE = 5,	/* total swap usage */
        VM_MAXID = 6		/* number of valid vm ids */
    }
}
