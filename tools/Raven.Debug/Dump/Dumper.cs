// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using McMaster.Extensions.CommandLineUtils;
using Microsoft.Diagnostics.NETCore.Client;
using Raven.Debug.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Utils;

namespace Microsoft.Diagnostics.Tools.Dump
{
    public partial class Dumper
    {
        /// <summary>
        /// The dump type determines the kinds of information that are collected from the process.
        /// </summary>
        public enum DumpTypeOption
        {
            Full,       // The largest dump containing all memory including the module images.

            Heap,       // A large and relatively comprehensive dump containing module lists, thread lists, all
                        // stacks, exception information, handle information, and all memory except for mapped images.

            Mini,       // A small dump containing module lists, thread lists, exception information and all stacks.

            Triage      // A small dump containing module lists, thread lists, exception information, all stacks and PII removed.
        }

        public Dumper()
        {
        }

        public int Collect(CommandLineApplication cmd, int processId, string output, string outputOwner, bool diag, bool crashreport, DumpTypeOption type)
        {
            if (processId == 0)
            {
                return cmd.ExitWithError("ProcessId is required.");
            }

            if (processId < 0)
            {
                return cmd.ExitWithError($"The PID cannot be negative: {processId}");
            }

            try
            {
                if (output == null)
                {
                    // Build timestamp based file path
                    string timestamp = $"{DateTime.Now:yyyyMMdd_HHmmss}";
                    output = Path.Combine(Directory.GetCurrentDirectory(), RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $"dump_{processId}_{timestamp}.dmp" : $"core_{processId}_{timestamp}");
                }
                // Make sure the dump path is NOT relative. This path could be sent to the runtime 
                // process on Linux which may have a different current directory.
                output = Path.GetFullPath(output);

                // Display the type of dump and dump path
                string dumpTypeMessage = null;
                switch (type)
                {
                    case DumpTypeOption.Full:
                        dumpTypeMessage = "full";
                        break;
                    case DumpTypeOption.Heap:
                        dumpTypeMessage = "dump with heap";
                        break;
                    case DumpTypeOption.Mini:
                        dumpTypeMessage = "dump";
                        break;
                    case DumpTypeOption.Triage:
                        dumpTypeMessage = "triage dump";
                        break;
                }
                cmd.Out.WriteLine($"Writing {dumpTypeMessage} to {output}");

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    if (crashreport)
                    {
                        Console.WriteLine("Crash reports not supported on Windows.");
                        return -1;
                    }

                    Windows.CollectDump(processId, output, type);
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    //DiagnosticsClient client = new(processId);

                    DumpType dumpType = DumpType.Normal;
                    switch (type)
                    {
                        case DumpTypeOption.Full:
                            dumpType = DumpType.Full;
                            break;
                        case DumpTypeOption.Heap:
                            dumpType = DumpType.WithHeap;
                            break;
                        case DumpTypeOption.Mini:
                            dumpType = DumpType.Normal;
                            break;
                        case DumpTypeOption.Triage:
                            dumpType = DumpType.Triage;
                            break;
                    }

                    WriteDumpFlags flags = WriteDumpFlags.None;
                    if (diag)
                    {
                        flags |= WriteDumpFlags.LoggingEnabled;
                    }
                    if (crashreport)
                    {
                        flags |= WriteDumpFlags.CrashReportEnabled;
                    }

                    var exec = "createdump";
                    if (Path.Exists(exec) == false)
                        throw new InvalidOperationException($"Could not find '{exec}' file.");

                    var argumentsString = $"-f {CommandLineArgumentEscaper.EscapeSingleArg(output)} {GetDumpTypeArgument(dumpType)} {GetWriteDumpFlags(flags)} --single-file {processId}";

                    Console.WriteLine($"{exec} {argumentsString}");

                    var processStartInfo = new ProcessStartInfo
                    {
                        FileName = exec,
                        Arguments = argumentsString,
                        CreateNoWindow = true,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        RedirectStandardInput = true,
                        UseShellExecute = false
                    };

                    // Send the command to the runtime to initiate the core dump
                    // client.WriteDump(dumpType, output, flags);

                    Process process = null;
                    try
                    {
                        process = Process.Start(processStartInfo);
                        process.EnableRaisingEvents = true;
                    }
                    catch (Exception e)
                    {
                        process?.Kill();
                        throw new InvalidOperationException($"Unable to execute {exec}.{Environment.NewLine}Command was: {Environment.NewLine}{processStartInfo.WorkingDirectory}> {processStartInfo.FileName} {processStartInfo.Arguments}", e);
                    }

                    process.WaitForExit();

                    static string GetWriteDumpFlags(WriteDumpFlags flags)
                    {
                        var sb = new StringBuilder();
                        if (flags.HasFlag(WriteDumpFlags.LoggingEnabled))
                            sb.Append(" --diag");

                        if (flags.HasFlag(WriteDumpFlags.CrashReportEnabled))
                            sb.Append(" --crashreport");

                        if (flags.HasFlag(WriteDumpFlags.VerboseLoggingEnabled))
                            sb.Append(" --verbose");

                        return sb.ToString();
                    }

                    static string GetDumpTypeArgument(DumpType type)
                    {
                        switch (type)
                        {
                            case DumpType.Normal:
                                return " --normal";
                            case DumpType.WithHeap:
                                return " --withheap";
                            case DumpType.Triage:
                                return " --triage";
                            case DumpType.Full:
                                return " --full";
                            default:
                                throw new ArgumentOutOfRangeException(nameof(type), type, null);
                        }
                    }

                    if (string.IsNullOrEmpty(outputOwner) == false)
                        PosixFileExtensions.ChangeFileOwner(output, outputOwner);
                }
                else
                {
                    throw new PlatformNotSupportedException($"Unsupported operating system: {RuntimeInformation.OSDescription}");
                }
            }
            catch (Exception ex) when
                (ex is FileNotFoundException or
                    ArgumentException or
                    DirectoryNotFoundException or
                    UnauthorizedAccessException or
                    PlatformNotSupportedException or
                    UnsupportedCommandException or
                    InvalidDataException or
                    InvalidOperationException or
                    NotSupportedException or
                    DiagnosticsClientException)
            {
                return cmd.ExitWithError($"{ex.Message}");
            }

            cmd.Out.WriteLine($"Complete");
            return 0;
        }
    }
}
