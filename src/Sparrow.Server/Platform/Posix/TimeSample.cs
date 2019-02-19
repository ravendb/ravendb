using System.Runtime.InteropServices;

namespace Sparrow.Server.Platform.Posix
{
    [StructLayout(LayoutKind.Sequential)]
    public struct TimeSample
    {
        public long tms_utime;  /* user time */
        public long tms_stime;  /* system time */
        public long tms_cutime; /* user time of children */
        public long tms_cstime; /* system time of children */
    };
    
    [StructLayout(LayoutKind.Sequential)]
    public struct TimeSample_32bit
    {
        public int tms_utime;  /* user time */
        public int  tms_stime;  /* system time */
        public int tms_cutime; /* user time of children */
        public int tms_cstime; /* system time of children */
    };
}
