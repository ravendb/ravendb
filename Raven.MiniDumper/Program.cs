using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using NDesk.Options;

namespace Raven.MiniDumper
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var program = new Program();
            program.Initialize();
            program.ParseArguments(args);

            program.AssertOptions();


            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;

            Console.WriteLine();
            program.Execute();
        }

        internal MiniDumperOptions options = new MiniDumperOptions();

        private OptionSet optionSet;

        private void Initialize()
        {
            optionSet = new OptionSet();
            optionSet.Add("process-id=", OptionCategory.None, "ProcessID to dump (Default = Look for Raven.Server.exe)", processId => options.ProcessId = int.Parse(processId));
            optionSet.Add("path-of-dump=", OptionCategory.None, "Path of output dump file (Default = Temp Directory)", path => options.DumpPath = path);
            optionSet.Add("path-of-pdbs=", OptionCategory.None, "Path to look for and copy all PDBs file (Default = Path of <process-id>)", path => options.PdbsPath = path);
            optionSet.Add("dump=", OptionCategory.None, "Set dump option which might be one or more (seperated with ',') of these : "
                + "Normal, WithDataSegs, WithFullMemory, WithHandleData, FilterMemory, ScanMemory, WithUnloadedModules, WithIndirectlyReferencedMemory, FilterModulePaths, "
                + "WithProcessThreadData, WithPrivateReadWriteMemory, WithoutOptionalData, WithoutOptionalData, WithFullMemoryInfo, WithThreadInfo, WithThreadInfo, WithCodeSegs, WithCodeSegs, "
                + "WithoutAuxiliaryState, WithoutAuxiliaryState, WithFullAuxiliaryState, WithFullAuxiliaryState, WithPrivateWriteCopyMemory, WithPrivateWriteCopyMemory, IgnoreInaccessibleMemory, "
                + "IgnoreInaccessibleMemory, ValidTypeFlags." + $"{Environment.NewLine}Default = WithThreadInfo,WithProcessThreadData", dumpOptions => options.DumpOption = options.SetDumpOptions(dumpOptions));
            optionSet.Add("h|?|help", OptionCategory.Help, string.Empty, v =>
            {
                PrintUsage();
                Environment.Exit((int)ExitCodes.Success);
            });            
        }

        private void AssertOptions()
        {
            if (options.ProcessId <= 0)
            {
                var proc = Process.GetProcessesByName("Raven.Server");
                switch (proc.Length)
                {
                    case 0:
                        Console.WriteLine("ProcessID (--process-id) is empty, and no Raven.Server process was found.");
                        Environment.Exit((int)ExitCodes.InvalidArguments);
                        break;

                    case 1:
                        NotifyAssumption($"--process-id was not specified, assuming you meant Raven.Server process {proc[0].Id}");
                        options.ProcessId = proc[0].Id;
                        if (options.PdbsPath == null)
                            options.PdbsPath = Path.GetDirectoryName(proc[0].MainModule.FileName);
                        break;
                    default:
                        Console.WriteLine("ProcessID (--process-id) is empty, and there is more than a single Raven.Server process in the system, specify which one to use!");
                        Environment.Exit((int)ExitCodes.InvalidArguments);
                        break;
                }
            }

            try
            {
                var proc = Process.GetProcessById(options.ProcessId);
                if (options.PdbsPath == null)
                    options.PdbsPath = Path.GetDirectoryName(proc.MainModule.FileName);
            }
            catch (Exception)
            {
                Console.WriteLine("Invalid processID.");
                Environment.Exit((int)ExitCodes.InvalidArguments);
            }
        }

        private static void NotifyAssumption(string msg)
        {
            var foregroundColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.DarkRed;
            Console.WriteLine(msg);
            Console.ForegroundColor = foregroundColor;
        }

        private void Execute()
        {
            Console.WriteLine($"Dumping - PID={options.ProcessId}, Dump Options={options.DumpOption}, Output Path={options.DumpPath}, PDBs Path={options.PdbsPath}...");

            string rc;
            try
            {
                var dumper = Database.Util.MiniDumper.Instance;
                rc = dumper.Write(options.DumpOption, options.DumpPath, options.ProcessId);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Failed to write dump : {ex.Message}");
                Console.ResetColor();
                return;
            }
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Dump written at {rc}");
            Console.ResetColor();

            DirectoryCopyPDBs(options.PdbsPath, Path.Combine(options.DumpPath, "PDBs"));
        }

        private void DirectoryCopyPDBs(string sourceDirName, string destDirName)
        {
            string extention = ".PDB";
            try
            {
                DirectoryInfo dir = new DirectoryInfo(sourceDirName);
                if (!dir.Exists)
                    throw new DirectoryNotFoundException($"Source PDB directory does not exist or could not be found: {options.PdbsPath}");

                DirectoryInfo[] dirs = dir.GetDirectories();

                if (IsFilesWithExtExistsInPath(dir, extention) == false)
                    return;

                if (!Directory.Exists(destDirName))
                    Directory.CreateDirectory(destDirName);

                FileInfo[] files = dir.GetFiles();
                foreach (FileInfo file in files)
                {
                    if (file.Extension.Equals(extention, StringComparison.OrdinalIgnoreCase))
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        string temppath = Path.Combine(destDirName, file.Name);
                        try
                        {
                            if (!File.Exists(temppath))
                                file.CopyTo(temppath, false);
                        }
                        catch (IOException ioException)
                        {
                            Console.WriteLine($"Failed to copy PDB file {temppath} : {ioException.Message}");
                        }
                        Console.ResetColor();
                    }
                }

                foreach (DirectoryInfo subdir in dirs)
                {
                    string temppath = Path.Combine(destDirName, subdir.Name);
                    DirectoryCopyPDBs(subdir.FullName, temppath);
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Failed to copy PDBs files: {ex.Message}");
                Console.ResetColor();
            }
        }

        private bool IsFilesWithExtExistsInPath(DirectoryInfo dirinfo, string extention)
        {
            FileInfo[] files = dirinfo.GetFiles();
            if (files.Any(f => f.Extension.Equals(extention, StringComparison.OrdinalIgnoreCase)))
                return true;

            DirectoryInfo[] dirs = dirinfo.GetDirectories();
            if (dirs.Any(d => IsFilesWithExtExistsInPath(d, extention) == true))
                return true;

            return false;
        }


        private void ParseArguments(string[] args)
        {
            try
            {
                if (args.Length == 0)
                    PrintUsage();

                optionSet.Parse(args);
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not understand arguments");
                Console.WriteLine(e.Message);
                PrintUsage();

                Environment.Exit((int)ExitCodes.InvalidArguments);
            }
        }

        private void PrintUsage()
        {
            Console.WriteLine(
                @"
Mini Dumper for RavenDB (64 bit only)
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------
Command line options:", DateTime.UtcNow.Year);

            optionSet.WriteOptionDescriptions(Console.Out);

            Console.WriteLine();
        }
    }
}
