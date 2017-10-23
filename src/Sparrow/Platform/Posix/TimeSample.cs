using System.Runtime.InteropServices;

namespace Sparrow.Platform.Posix
{
    [StructLayout(LayoutKind.Sequential)]
    public struct TimeSample
    {
        public long tms_utime;  /* user time */
        public long tms_stime;  /* system time */
        public long tms_cutime; /* user time of children */
        public long tms_cstime; /* system time of children */
    };
}
