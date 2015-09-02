using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Diagnostics.Tracing.Parsers.MicrosoftWindowsTCPIP;

namespace Raven.Monitor.Memory
{
	internal class MemoryPerformanceMonitor : IMonitor
	{
		public MemoryPerformanceMonitor(MonitorOptions options)

		{
			_pid = options.ProcessId;
		}

		private MemoryUssageReportDoc reportDoc;
		public void Start()
		{
			reportDoc = new MemoryUssageReportDoc(){StartTime = DateTime.UtcNow, Sampling = new List<MemoryUssageSample>()};
		}

		public void Stop()
		{
			reportDoc.EndTime = DateTime.UtcNow;
			using (var session = RavenDocumentStore.DocumentStore.OpenSession())
			{
				session.Store(reportDoc);
				session.SaveChanges();
			}
		}

		public void OnTimerTick()
		{
			var sample = new MemoryUssageSample() {Time = DateTime.UtcNow, UsedMemoryInBytes = GetCurrentWorkingSet()};
			reportDoc.Sampling.Add(sample);
		}

		public long GetCurrentWorkingSet()
        {
            PROCESS_MEMORY_COUNTERS pr;

			if (GetProcessMemoryInfo(Process.GetProcessById(_pid).Handle, out pr, 40) == false)
                throw new Win32Exception();

            return (long)pr.WorkingSetSize.ToUInt64();
        }

		[DllImport("psapi.dll", SetLastError = true)]
        static extern bool GetProcessMemoryInfo(IntPtr hProcess, out PROCESS_MEMORY_COUNTERS counters, uint size);

        [StructLayout(LayoutKind.Sequential, Size = 40)]
        // ReSharper disable once InconsistentNaming - Win32 API
        public struct PROCESS_MEMORY_COUNTERS
        {
            public uint cb;             // The size of the structure, in bytes (DWORD).
            public uint PageFaultCount;         // The number of page faults (DWORD).
            public UIntPtr PeakWorkingSetSize;     // The peak working set size, in bytes (SIZE_T).
            public UIntPtr WorkingSetSize;         // The current working set size, in bytes (SIZE_T).
            public UIntPtr QuotaPeakPagedPoolUsage;    // The peak paged pool usage, in bytes (SIZE_T).
            public UIntPtr QuotaPagedPoolUsage;    // The current paged pool usage, in bytes (SIZE_T).
            public UIntPtr QuotaPeakNonPagedPoolUsage; // The peak nonpaged pool usage, in bytes (SIZE_T).
            public UIntPtr QuotaNonPagedPoolUsage;     // The current nonpaged pool usage, in bytes (SIZE_T).
            public UIntPtr PagefileUsage;          // The Commit Charge value in bytes for this process (SIZE_T). Commit Charge is the total amount of memory that the memory manager has committed for a running process.
            public UIntPtr PeakPagefileUsage;      // The peak value in bytes of the Commit Charge during the lifetime of this process (SIZE_T).
        }

		private readonly int _pid;
		private Timer _timer;
		public void Dispose()
		{
			if (_timer != null)
				_timer.Dispose();
			reportDoc = null;
		}
	}
}
