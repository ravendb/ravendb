using System;
using System.Diagnostics;

namespace Raven.Client.Server.Debugging
{
    internal class ThreadInfo
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime StartingTime { get; set; }
        public ThreadState State { get; set; }
        public ThreadWaitReason? WaitReason { get; set; }
        public TimeSpan TotalProcessorTime { get; set; }
        public TimeSpan PrivilegedProcessorTime { get; set; }
        public TimeSpan UserProcessorTime { get; set; }
    }
}
