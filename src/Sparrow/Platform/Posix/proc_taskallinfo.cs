using System.Runtime.InteropServices;

namespace Sparrow.Platform.Posix
{
    // from proc_info.h
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    public struct proc_taskallinfo
    {
        public proc_bsdinfo pbsd;
        public proc_taskinfo ptinfo;
    }

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct proc_bsdinfo
    {
        internal uint pbi_flags;
        internal uint pbi_status;
        internal uint pbi_xstatus;
        internal uint pbi_pid;
        internal uint pbi_ppid;
        internal uint pbi_uid;
        internal uint pbi_gid;
        internal uint pbi_ruid;
        internal uint pbi_rgid;
        internal uint pbi_svuid;
        internal uint pbi_svgid;
        internal uint reserved;
        internal fixed byte pbi_comm[16]; // MAXCOMLEN = 16
        internal fixed byte pbi_name[16 * 2]; // MAXCOMLEN = 16
        internal uint pbi_nfiles;
        internal uint pbi_pgid;
        internal uint pbi_pjobc;
        internal uint e_tdev;
        internal uint e_tpgid;
        internal int pbi_nice;
        internal ulong pbi_start_tvsec;
        internal ulong pbi_start_tvusec;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct proc_taskinfo
    {
        public ulong pti_virtual_size;
        public ulong pti_resident_size;
        public ulong pti_total_user;   // in nanoseconds
        public ulong pti_total_system; // in nanoseconds
        public ulong pti_threads_user;
        public ulong pti_threads_system;
        public int pti_policy;
        public int pti_faults;
        public int pti_pageins;
        public int pti_cow_faults;
        public int pti_messages_sent;
        public int pti_messages_received;
        public int pti_syscalls_mach;
        public int pti_syscalls_unix;
        public int pti_csw;
        public int pti_threadnum;
        public int pti_numrunning;
        public int pti_priority;
    };

    public enum ProcessInfo
    {
        PROC_PIDTASKALLINFO = 2,
        PROC_PIDTHREADINFO = 5,
        PROC_PIDLISTTHREADS = 6
    }
}
