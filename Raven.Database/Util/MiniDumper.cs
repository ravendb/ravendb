using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Client.Connection;

namespace Raven.Database.Util
{
    public class MiniDumper : IDisposable
    {
        private Timer timer;
        private int counts;
        private int period;
        private int currentCount;
        private bool additionalStats;
        private string url;
        private Option options;
        private static volatile MiniDumper instance;
        private static object syncRoot = new Object();
        

        private MiniDumper() { }

        public static MiniDumper Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (syncRoot)
                    {
                        if (instance == null)
                            instance = new MiniDumper();
                    }
                }
                return instance;
            }
        }

        public static string PrintUsage()
        {
            return $"Usage: {Environment.NewLine}admin/dump/usage=1{Environment.NewLine}{Environment.NewLine}admin/dump/stop=1 (cancel timer){Environment.NewLine}{Environment.NewLine}"
                + $"admin/dump[?option=<DumpOption>...] (You may combine few options).{Environment.NewLine}"
                + $"   Example: admin/dump?option=WithFullMemory&option=WithProcessThreadData   (default is 'Normal', and for full dump you may use 'ValidTypeFlags'){Environment.NewLine}{Environment.NewLine}"
                + $"admin/dump?timer=<count>&period=<minutes>[&option=<DumpOption>...][&stats=1] (Stats for addtional info file){Environment.NewLine}"
                + $"   Example: admin/dump?timer=96&period=5&option=WithFullMemory&stats=1{Environment.NewLine}{Environment.NewLine}"
                + "Valid DumpOption: Normal, WithDataSegs, WithFullMemory, WithHandleData, FilterMemory, ScanMemory, WithUnloadedModules, WithIndirectlyReferencedMemory, FilterModulePaths, "
                + "WithProcessThreadData, WithPrivateReadWriteMemory, WithoutOptionalData, WithoutOptionalData, WithFullMemoryInfo, WithThreadInfo, WithThreadInfo, WithCodeSegs, WithCodeSegs, "
                + "WithoutAuxiliaryState, WithoutAuxiliaryState, WithFullAuxiliaryState, WithFullAuxiliaryState, WithPrivateWriteCopyMemory, WithPrivateWriteCopyMemory, IgnoreInaccessibleMemory, "
                + "IgnoreInaccessibleMemory, ValidTypeFlags";
        }

        [Flags]
        public enum Option : uint
        {
            // From dbghelp.h:
            Normal = 0x00000000,
            WithDataSegs = 0x00000001,
            WithFullMemory = 0x00000002,
            WithHandleData = 0x00000004,
            FilterMemory = 0x00000008,
            ScanMemory = 0x00000010,
            WithUnloadedModules = 0x00000020,
            WithIndirectlyReferencedMemory = 0x00000040,
            FilterModulePaths = 0x00000080,
            WithProcessThreadData = 0x00000100,
            WithPrivateReadWriteMemory = 0x00000200,
            WithoutOptionalData = 0x00000400,
            WithFullMemoryInfo = 0x00000800,
            WithThreadInfo = 0x00001000,
            WithCodeSegs = 0x00002000,
            WithoutAuxiliaryState = 0x00004000,
            WithFullAuxiliaryState = 0x00008000,
            WithPrivateWriteCopyMemory = 0x00010000,
            IgnoreInaccessibleMemory = 0x00020000,
            ValidTypeFlags = 0x0003ffff,
        };

        public static Option StringToOption(string id)
        {
            Option option;
            switch (id)
            {
                case "Normal":
                    option = MiniDumper.Option.Normal;
                    break;
                case "WithDataSegs":
                    option = MiniDumper.Option.WithDataSegs;
                    break;
                case "WithFullMemory":
                    option = MiniDumper.Option.WithFullMemory;
                    break;
                case "WithHandleData":
                    option = MiniDumper.Option.WithHandleData;
                    break;
                case "FilterMemory":
                    option = MiniDumper.Option.FilterMemory;
                    break;
                case "ScanMemory":
                    option = MiniDumper.Option.ScanMemory;
                    break;
                case "WithUnloadedModules":
                    option = MiniDumper.Option.WithUnloadedModules;
                    break;
                case "WithIndirectlyReferencedMemory":
                    option = MiniDumper.Option.WithIndirectlyReferencedMemory;
                    break;
                case "FilterModulePaths":
                    option = MiniDumper.Option.FilterModulePaths;
                    break;
                case "WithProcessThreadData":
                    option = MiniDumper.Option.WithProcessThreadData;
                    break;
                case "WithPrivateReadWriteMemory":
                    option = MiniDumper.Option.WithPrivateReadWriteMemory;
                    break;
                case "WithoutOptionalData":
                    option = MiniDumper.Option.WithoutOptionalData;
                    break;
                case "WithFullMemoryInfo":
                    option = MiniDumper.Option.WithFullMemoryInfo;
                    break;
                case "WithThreadInfo":
                    option = MiniDumper.Option.WithThreadInfo;
                    break;
                case "WithCodeSegs":
                    option = MiniDumper.Option.WithCodeSegs;
                    break;
                case "WithoutAuxiliaryState":
                    option = MiniDumper.Option.WithoutAuxiliaryState;
                    break;
                case "WithFullAuxiliaryState":
                    option = MiniDumper.Option.WithFullAuxiliaryState;
                    break;
                case "WithPrivateWriteCopyMemory":
                    option = MiniDumper.Option.WithPrivateWriteCopyMemory;
                    break;
                case "IgnoreInaccessibleMemory":
                    option = MiniDumper.Option.IgnoreInaccessibleMemory;
                    break;
                case "ValidTypeFlags":
                    option = MiniDumper.Option.ValidTypeFlags;
                    break;
                default:
                    option = MiniDumper.Option.Normal;
                    break;
            }
            return option;
        }


        public enum ExceptionInfo
        {
            None,
            Present
        }

        //typedef struct _MINIDUMP_EXCEPTION_INFORMATION {
        //    DWORD ThreadId;
        //    PEXCEPTION_POINTERS ExceptionPointers;
        //    BOOL ClientPointers;
        //} MINIDUMP_EXCEPTION_INFORMATION, *PMINIDUMP_EXCEPTION_INFORMATION;
        [StructLayout(LayoutKind.Sequential, Pack = 4)] // Pack=4 is important! So it works also for x64!
        private struct MiniDumpExceptionInformation
        {
            public uint ThreadId;
            public IntPtr ExceptionPointers;
            [MarshalAs(UnmanagedType.Bool)]
            public bool ClientPointers;
        }

        //BOOL
        //WINAPI
        //MiniDumpWriteDump(
        //    __in HANDLE hProcess,
        //    __in DWORD ProcessId,
        //    __in HANDLE hFile,
        //    __in MINIDUMP_TYPE DumpType,
        //    __in_opt PMINIDUMP_EXCEPTION_INFORMATION ExceptionParam,
        //    __in_opt PMINIDUMP_USER_STREAM_INFORMATION UserStreamParam,
        //    __in_opt PMINIDUMP_CALLBACK_INFORMATION CallbackParam
        //    );

        // Overload requiring MiniDumpExceptionInformation
        [DllImport("dbghelp.dll",
            EntryPoint = "MiniDumpWriteDump",
            CallingConvention = CallingConvention.StdCall,
            CharSet = CharSet.Unicode,
            ExactSpelling = true,
            SetLastError = true)]
        private static extern bool MiniDumpWriteDump(IntPtr hProcess,
            uint processId,
            SafeHandle hFile,
            uint dumpType,
            ref MiniDumpExceptionInformation expParam,
            IntPtr userStreamParam,
            IntPtr callbackParam);

        // Overload supporting MiniDumpExceptionInformation == NULL
        [DllImport("dbghelp.dll",
            EntryPoint = "MiniDumpWriteDump",
            CallingConvention = CallingConvention.StdCall,
            CharSet = CharSet.Unicode,
            ExactSpelling = true,
            SetLastError = true)]
        private static extern bool MiniDumpWriteDump(IntPtr hProcess,
            uint processId,
            SafeHandle hFile,
            uint dumpType,
            IntPtr expParam,
            IntPtr userStreamParam,
            IntPtr callbackParam);

        [DllImport("kernel32.dll", EntryPoint = "GetCurrentThreadId", ExactSpelling = true)]
        private static extern uint GetCurrentThreadId();

        private static bool Write(SafeHandle fileHandle, Option options, ExceptionInfo exceptionInfo)
        {
            Process currentProcess = Process.GetCurrentProcess();
            IntPtr currentProcessHandle = currentProcess.Handle;
            uint currentProcessId = (uint)currentProcess.Id;
            MiniDumpExceptionInformation exp;
            exp.ThreadId = GetCurrentThreadId();
            exp.ClientPointers = false;
            exp.ExceptionPointers = IntPtr.Zero;
            if (exceptionInfo == ExceptionInfo.Present)
            {
                exp.ExceptionPointers = System.Runtime.InteropServices.Marshal.GetExceptionPointers();
            }
            bool bRet = false;
            if (exp.ExceptionPointers == IntPtr.Zero)
            {
                bRet = MiniDumpWriteDump(currentProcessHandle, currentProcessId, fileHandle, (uint)options, IntPtr.Zero,
                    IntPtr.Zero, IntPtr.Zero);
            }
            else
            {
                bRet = MiniDumpWriteDump(currentProcessHandle, currentProcessId, fileHandle, (uint)options, ref exp,
                    IntPtr.Zero, IntPtr.Zero);
            }
            return bRet;
        }

        public string Write(Option? passedOptions = null)
        {
            if (passedOptions != null)
                options = passedOptions.Value;
            var dumpDir = Path.Combine(Path.GetTempPath(), "RavenDBDumps");
            if (Directory.Exists(dumpDir) == false)
                Directory.CreateDirectory(dumpDir);

            var dumpFile = Path.Combine(dumpDir, $"RavenDBDump{DateTime.UtcNow.ToString("yyyyMMdd_hhmmss")}.dmp");

            using (FileStream fs = new FileStream(dumpFile, FileMode.Create, FileAccess.ReadWrite, FileShare.Write))
                Write(fs.SafeFileHandle, options, MiniDumper.ExceptionInfo.Present);

            if (additionalStats == true)
            {
                string logFile = Path.Combine(dumpDir, $"RavenDBDump{DateTime.UtcNow.ToString("yyyyMMdd_hhmmss")}.log");
                try
                {
                    using (TextWriter tw = File.CreateText(logFile))
                    {
                        var eps = new string[]
                        {
                            "debug/cache-details",
                            "debug/prefetch-status",
                            "debug/indexing-perf-stats",
                            "debug/indexing-batch-stats",
                            "debug/reducing-batch-stats",
                            "debug/currently-indexing",
                            "debug/clear-remaining-reductions",
                            "debug/metrics",
                            "debug/sql-replication-perf-stats",
                            "debug/replication-perf-stats",
                            "debug/sql-replication-stats"
                        };

                        foreach (var ep in eps)
                        {
                            try
                            {
                                using (var request = new HttpJsonRequestFactory(1024).CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, $"{url}/{ep}", HttpMethod.Get, null, null)))
                                {
                                    var json = request.ReadResponseJson();
                                    tw.WriteLine($"EP : {ep}");
                                    tw.WriteLine(json.ToString());
                                }
                            }
                            catch (Exception e)
                            {
                                tw.WriteLine($"Failed to EP : {ep}.  Exception = ${e.Message}");
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    // ignored
                }
            }

            return dumpFile;
        }

        public string StartTimer(int count, int period, Option options, bool addAddtionalStats, string url)
        {
            StopTimer();
            this.counts = count;
            this.period = period;
            this.options = options;
            this.additionalStats = addAddtionalStats;
            this.url = url;
            timer = new Timer(Execute, null, TimeSpan.FromMinutes(period), TimeSpan.FromMinutes(period));
            var filepath = Write();
            return $"Timer set with count={count}, period={period}{Environment.NewLine}Starting with {filepath}";
        }

        private void Execute(object state)
        {
            if (++currentCount > counts || timer == null)
            {
                StopTimer();
                return;
            }

            Write();
        }

        public void StopTimer()
        {
            currentCount = 0;
            if (timer != null)
            {
                var copy = timer;
                timer = null;
                copy.Dispose();
            }
        }

        public void Dispose()
        {
            StopTimer();
        }
    }
}
