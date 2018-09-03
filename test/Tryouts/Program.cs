using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Server.Replication;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using Microsoft.Win32.SafeHandles;
using RachisTests;
using RachisTests.DatabaseCluster;
using Raven.Client.Documents.Queries;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Authentication;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Client.Attachments;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Utils;
using StressTests.Client.Attachments;
using Xunit;

namespace Tryouts
{
    public static class Program
    {
        private static ManualResetEventSlim _isRunningEvent;

        public static void Main(string[] args)
        {            
            _isRunningEvent = new ManualResetEventSlim();
            HashSet<ulong> threadIds = null;
            long lowMemoryCount = 0;
            using(_isRunningEvent)
            {
                try
                {
                    using (var test = new ReplicationTests())
                    {
                        test.VerySimpleTest();
                    }

                    var sw = Stopwatch.StartNew();
                    double lowMemoryPerSec = 0;
                    TestBase.GlobalServer.ServerStore.LowMemoryNotification.LowMemoryEvent += () =>
                    {
                        lowMemoryCount++;
                        lowMemoryPerSec = lowMemoryCount / sw.Elapsed.TotalSeconds;
                    };
                    for (int i = 0; i < 1000; i++)
                    {                     
                        try
                        {
                            using (var test = new ReplicationTests())
                            {
                                //test.EnsureReplicationToWatchers(false).Wait();
                                test.VerySimpleTest();

                                var createdServersCount = test.Servers.Count;
                                Console.Clear();
                                Console.WriteLine("Iteration: " + i);                                
                                Console.WriteLine($"Low memory handlers activated {lowMemoryCount} times. (averaging on {lowMemoryPerSec:F} activations/sec)");
                                Console.WriteLine($"Server instances in test fixture : {createdServersCount}");
                                
                                var allocationsPerThread = Raven.Server.Program.WriteServerStatsOnce(test.Server).AllocationsPerThread;
                                if (threadIds != null)
                                {
                                    threadIds.IntersectWith(allocationsPerThread.Select(x => x.ThreadId));
                                    var newThreads = new HashSet<ulong>(allocationsPerThread.Select(x => x.ThreadId));
                                    newThreads.ExceptWith(threadIds);

                                    Console.WriteLine($"There are {allocationsPerThread.Count - threadIds.Count} new threads out of total {allocationsPerThread.Count} threads in this iteration.");

                                    Console.WriteLine("New threads:");
                                    foreach (var id in newThreads)
                                    {
                                        Console.WriteLine(allocationsPerThread.FirstOrDefault(x => x.ThreadId == id).ThreadName);
                                    }
                                }
                                threadIds = new HashSet<ulong>(allocationsPerThread.Select(x => x.ThreadId));
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            break;
                        }

                        LowMemoryNotification.Instance.SimulateLowMemoryNotification();
//                        GC.Collect(2);
                    }
                }
                finally
                {
                    _isRunningEvent.Set();
                }
            }
        }
    }

   public static class DumpHelper
    {
        public enum DumpType
        {
            MiniDumpNormal = 0,
            MiniDumpWithDataSegs = 1,
            MiniDumpWithFullMemory = 2,
            MiniDumpWithHandleData = 4,
            MiniDumpFilterMemory = 8,
            MiniDumpScanMemory = 16,
            MiniDumpWithUnloadedModules = 32,
            MiniDumpWithIndirectlyReferencedMemory = 64,
            MiniDumpFilterModulePaths = 128,
            MiniDumpWithProcessThreadData = 256,
            MiniDumpWithPrivateReadWriteMemory = 512,
            MiniDumpWithoutOptionalData = 1024,
            MiniDumpWithFullMemoryInfo = 2048,
            MiniDumpWithThreadInfo = 4096,
            MiniDumpWithCodeSegs = 8192,
        }

        [DllImportAttribute("dbghelp.dll")]
        [return: MarshalAsAttribute(UnmanagedType.Bool)]
        private static extern bool MiniDumpWriteDump(
            [In] IntPtr hProcess,
            uint ProcessId,
            SafeFileHandle hFile,
            DumpType DumpType,
            [In] IntPtr ExceptionParam,
            [In] IntPtr UserStreamParam,
            [In] IntPtr CallbackParam);

        public static void WriteTinyDumpForThisProcess(string fileName)
        {
            WriteDumpForThisProcess(fileName, DumpType.MiniDumpNormal);
        }

        public static void WriteFullDumpForThisProcess(string fileName)
        {
            WriteDumpForThisProcess(fileName, DumpType.MiniDumpWithFullMemory);
        }

        public static void WriteDumpForThisProcess(string fileName, DumpType dumpType)
        {
            WriteDumpForProcess(Process.GetCurrentProcess(), fileName, dumpType);
        }

        public static void WriteTinyDumpForProcess(Process process, string fileName)
        {
            WriteDumpForProcess(process, fileName, DumpType.MiniDumpNormal);
        }

        public static void WriteFullDumpForProcess(Process process, string fileName)
        {
            WriteDumpForProcess(process, fileName, DumpType.MiniDumpWithFullMemory);
        }

        public static void WriteDumpForProcess(Process process, string fileName, DumpType dumpType)
        {
            using (FileStream fs = File.Create(fileName))
            {
                if (!MiniDumpWriteDump(Process.GetCurrentProcess().Handle,
                    (uint)process.Id, fs.SafeFileHandle, dumpType,
                    IntPtr.Zero, IntPtr.Zero, IntPtr.Zero))
                {
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "Error calling MiniDumpWriteDump.");
                }
            }
        }
    }
}
