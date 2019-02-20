namespace Sparrow.Platform.Posix.macOS
{
    internal enum CtkHwIdentifiers
    {
        HW_MACHINE = 1,         /* string: machine class */
        HW_MODEL = 2,           /* string: specific machine model */
        HW_NCPU = 3,            /* int: number of cpus */
        HW_BYTEORDER = 4,       /* int: machine byte order */
        HW_PHYSMEM = 5,         /* int: total memory */
        HW_USERMEM = 6,	        /* int: non-kernel memory */
        HW_PAGESIZE = 7,        /* int: software page size */
        HW_DISKNAMES = 8,       /* strings: disk drive names */
        HW_DISKSTATS = 9,       /* struct: diskstats[] */
        HW_EPOCH = 10,          /* int: 0 for Legacy, else NewWorld */
        HW_FLOATINGPT = 11,     /* int: has HW floating point? */
        HW_MACHINE_ARCH = 12,   /* string: machine architecture */
        HW_VECTORUNIT = 13,     /* int: has HW vector unit? */
        HW_BUS_FREQ = 14,       /* int: Bus Frequency */
        HW_CPU_FREQ = 15,       /* int: CPU Frequency */
        HW_CACHELINE = 16,      /* int: Cache Line Size in Bytes */
        HW_L1ICACHESIZE = 17,   /* int: L1 I Cache Size in Bytes */
        HW_L1DCACHESIZE = 18,   /* int: L1 D Cache Size in Bytes */
        HW_L2SETTINGS = 19,     /* int: L2 Cache Settings */
        HW_L2CACHESIZE = 20,    /* int: L2 Cache Size in Bytes */
        HW_L3SETTINGS = 21,     /* int: L3 Cache Settings */
        HW_L3CACHESIZE = 22,    /* int: L3 Cache Size in Bytes */
        HW_TB_FREQ = 23,        /* int: Bus Frequency */
        HW_MEMSIZE = 24,        /* uint64_t: physical ram size */
        HW_AVAILCPU = 25,       /* int: number of available CPUs */
        HW_MAXID = 26           /* number of valid hw ids */
    }
}
