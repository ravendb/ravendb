using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;
using System.Security.Permissions;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions;

namespace Raven.StorageExporter
{
    public class Program
    {
        static void Main(string[] args)
        {
            StorgaeExporterConfiguration configuration;
            var valid = ValidateArgs(args, out configuration);
            if (!valid)
            {
                PrintUsage();
                Console.Read();                
            }
            configuration.Export();            
        }

        private static bool ValidateArgs(string[] args, out StorgaeExporterConfiguration configuration)
        {
            configuration = null;
            if (args.Count() < 2)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Not enough arguments were provided.\n");
                return false;
            }
            if (!Directory.Exists(args[0]))
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Directory {0} does not exists.\n",args[0]);                
                return false; 
            }
            if (!StorageExporter.ValidateStorageExsist(args[0]))
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Directory {0} is not a valid RavenDB storage directory.\n", args[0]);
                return false; 
            }
            var outputDirectory = Path.GetDirectoryName(args[1]);
            if (!Directory.Exists(outputDirectory))
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Output directory {0} does not exists.\n", outputDirectory);
                return false;
            }
            var permissionSet = new PermissionSet(PermissionState.None);
            var writePermission = new FileIOPermission(FileIOPermissionAccess.Write, args[1]);
            permissionSet.AddPermission(writePermission);

            if (!permissionSet.IsSubsetOf(AppDomain.CurrentDomain.PermissionSet))
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "You don't have premissions to write to {0}.\n", args[1]);
            }
            if (args.Length % 2 != 0)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Wrong amount of arguments were passed.\n");
                return false;
            }           
            var currArgPos = 2;
            configuration = new StorgaeExporterConfiguration() {DatabaseDataDir = args[0], OutputDumpPath = args[1]};
            while (currArgPos < args.Length)
            {
                switch (args[currArgPos])
                {
                    case "-T":
                        configuration.TableName = args[currArgPos + 1];
                        currArgPos += 2;
                        break;
                    case "-BatchSize":
                        int batchSize;
                        if (int.TryParse(args[currArgPos + 1], out batchSize) && batchSize > 0)
                        {
                            configuration.BatchSize = batchSize;
                            currArgPos += 2;
                        }
                        else
                        {
                            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "BatchSize should be an integer number greater than 0 (BatchSize={0}).\n", args[currArgPos + 1]);
                            return false;
                        }
                        break;
                    default:
                        ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Unidentified argument {0}.\n");
                        break;
                }
            }
            return true;
        }

        private static void PrintUsage()
        {
            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.DarkMagenta, @"
Data ExportDatabase utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------", SystemTime.UtcNow.Year);

            Console.WriteLine(@"
Usage:
  - Export a RavenDB database to a file in raven.dump format.
	Raven.StorgaeExporter.exe c:\RavenDB\Databases\Northwind\ c:\RavenDB\Dumps\Northwind\northwind.raven [-BatchSize]
  - Export an ESENT table from given RavenDB database into a CSV formated file.
	Raven.StorgaeExporter.exe c:\RavenDB\Databases\Northwind\ c:\RavenDB\Dumps\Northwind\ref.csv -T ref 

Parameters:
 -T <TableName> : The name of the table to be exported.
 -BatchSize <integer number> : The size of the export batch (defualt size is 1024). 
 ");
            Console.WriteLine();
        }
    }
}
