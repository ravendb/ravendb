namespace Sparrow.Platform.Posix.macOS
{
    internal struct vm_statistics64
    {
        uint free_count;        /* # of pages free */
        uint active_count;      /* # of pages active */
        uint inactive_count;    /* # of pages inactive */
        uint wire_count;        /* # of pages wired down */
        ulong zero_fill_count;  /* # of zero fill pages */
        ulong reactivations;    /* # of pages reactivated */
        ulong pageins;          /* # of pageins */
        ulong pageouts;         /* # of pageouts */
        ulong faults;           /* # of faults */
        ulong cow_faults;       /* # of copy-on-writes */
        ulong lookups;          /* object cache lookups */
        ulong hits;             /* object cache hits */

        /* added for rev1 */
        ulong purges;           /* # of pages purged */
        uint purgeable_count;   /* # of pages purgeable */

        /* added for rev2 */
        /*
         * NB: speculative pages are already accounted for in "free_count",
         * so "speculative_count" is the number of "free" pages that are
         * used to hold data that was read speculatively from disk but
         * haven't actually been used by anyone so far.
         */
        uint speculative_count; /* # of pages speculative */

        public uint FreePagesCount => free_count;
        public uint ActivePagesCount => active_count;
        public uint InactivePagesCount => inactive_count;
        public uint WirePagesCount => wire_count;
    }
}
