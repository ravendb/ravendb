// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Raven.Debug.Utils;
using Sparrow;

namespace Microsoft.Diagnostics.Tools.GCDump
{
    internal static class GCHeapDumper
    {
        delegate Task<int> CollectDelegate(CancellationToken ct, IConsole console, int processId, string output, int timeout, bool verbose);

        /// <summary>
        /// Collects a gcdump from a currently running process.
        /// </summary>
        /// <param name="ct">The cancellation token</param>
        /// <param name="console"></param>
        /// <param name="processId">The process to collect the gcdump from.</param>
        /// <param name="output">The output path for the collected gcdump.</param>
        /// <returns></returns>
        public static async Task<int> Collect(CancellationToken ct, CommandLineApplication cmd, int processId, string output, int timeout, bool verbose)
        {
            try
            {
                if (processId < 0)
                {
                    return cmd.ExitWithError($"The PID cannot be negative: {processId}");
                }

                if (processId == 0)
                {
                    return cmd.ExitWithError($"-p|--process-id is required");
                }

                output = string.IsNullOrEmpty(output) ?
                    $"{DateTime.Now.ToString(@"yyyyMMdd\_hhmmss")}_{processId}.gcdump" :
                    output;

                FileInfo outputFileInfo = new FileInfo(output);

                if (outputFileInfo.Exists)
                {
                    outputFileInfo.Delete();
                }

                if (string.IsNullOrEmpty(outputFileInfo.Extension) || outputFileInfo.Extension != ".gcdump")
                {
                    outputFileInfo = new FileInfo(outputFileInfo.FullName + ".gcdump");
                }

                cmd.Out.WriteLine($"Writing gcdump to '{outputFileInfo.FullName}'...");
                var dumpTask = Task.Run(() =>
                {
                    var memoryGraph = new Graphs.MemoryGraph(50_000);
                    var heapInfo = new DotNetHeapInfo();
                    if (!EventPipeDotNetHeapDumper.DumpFromEventPipe(ct, processId, memoryGraph, verbose ? cmd.Out : TextWriter.Null, timeout, heapInfo))
                        return false;
                    memoryGraph.AllowReading();
                    GCHeapDump.WriteMemoryGraph(memoryGraph, outputFileInfo.FullName, "dotnet-gcdump");
                    return true;
                });

                var fDumpSuccess = await dumpTask;

                if (fDumpSuccess)
                {
                    outputFileInfo.Refresh();
                    cmd.Out.WriteLine($"\tFinished writing {new Size(outputFileInfo.Length, SizeUnit.Bytes)}.");
                    return 0;
                }
                else if (ct.IsCancellationRequested)
                {
                    cmd.Out.WriteLine($"\tCancelled.");
                    return -1;
                }
                else
                {
                    cmd.Out.WriteLine($"\tFailed to collect gcdump. Try running with '-v' for more information.");
                    return -1;
                }
            }
            catch (Exception ex)
            {
                return cmd.ExitWithError($"[ERROR] {ex}");
            }
        }
    }
}
