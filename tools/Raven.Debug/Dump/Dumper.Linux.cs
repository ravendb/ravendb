// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Diagnostics;
using System.Threading.Tasks;
using System.IO;

namespace Microsoft.Diagnostics.Tools.Dump
{
    public partial class Dumper
    {
        private static class Linux
        {
            internal static async Task CollectDumpAsync(Process process, string fileName, DumpTypeOption type)
            {
                // We don't work on WSL :(
                string ostype = await File.ReadAllTextAsync("/proc/sys/kernel/osrelease");
                if (ostype.Contains("Microsoft"))
                {
                    throw new PlatformNotSupportedException("Cannot collect memory dumps from Windows Subsystem for Linux.");
                }

                // First step is to find the .NET runtime. To do this we look for coreclr.so
                ProcessModule coreclr = process.Modules.Cast<ProcessModule>().FirstOrDefault(m => string.Equals(m.ModuleName, "libcoreclr.so"));
                if (coreclr == null)
                {
                    throw new NotSupportedException("Unable to locate .NET runtime associated with this process!");
                }

                // Find createdump next to that file
                string runtimeDirectory = Path.GetDirectoryName(coreclr.FileName);
                string createDumpPath = Path.Combine(runtimeDirectory, "createdump");
                if (!File.Exists(createDumpPath))
                {
                    throw new NotSupportedException($"Unable to locate 'createdump' tool in '{runtimeDirectory}'");
                }

                // Create the dump
                int exitCode = await CreateDumpAsync(createDumpPath, fileName, process.Id, type);
                if (exitCode != 0)
                {
                    throw new InvalidOperationException($"createdump exited with non-zero exit code: {exitCode}");
                }
            }

            private static Task<int> CreateDumpAsync(string exePath, string fileName, int processId, DumpTypeOption type)
            {
                string dumpType = type == DumpTypeOption.Mini ? "--normal" : "--withheap";
                var tcs = new TaskCompletionSource<int>();
                var createdump = new Process()
                {
                    StartInfo = new ProcessStartInfo()
                    {
                        FileName = exePath,
                        Arguments = $"--name {fileName} {dumpType} {processId}",
                    },
                    EnableRaisingEvents = true,
                };
                createdump.Exited += (s, a) => tcs.TrySetResult(createdump.ExitCode);
                createdump.Start();
                return tcs.Task;
            }
        }
    }
}
