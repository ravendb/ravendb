using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions;

namespace Raven.DataExporter
{
    public class Program
    {
        static void Main(string[] args)
        {
            DataExporterConfiguration configuration;
            var valid = ValidateArgs(args, out configuration);
            if (!valid)
            {
                PrintUsage();
                Console.Read();
                return;
            }
            var exporter = new DataExporter(configuration);
            exporter.Export();
        }

        private static bool ValidateArgs(string[] args, out DataExporterConfiguration configuration)
        {
            configuration = null;
            if (args.Count() < 2)
            {
                ConsoleWriteLineWithColor(ConsoleColor.Red, "Not enough arguments were provided.\n");
                return false;
            }
            if (!Directory.Exists(args[0]))
            {
                ConsoleWriteLineWithColor(ConsoleColor.Red, "Directory {0} does not exists.\n",args[0]);
                return false; 
            }
            if (args[1].Equals("-T"))
            {
                if (args.Count() < 4)
                {
                    ConsoleWriteLineWithColor(ConsoleColor.Red, "Not enough arguments were provided, expecting table name and dump folder after -T.\n");
                    return false;
                }
                configuration = new DataExporterConfiguration(){DatabaseDataDir = args[0],OutputDumpPath = args[3],TableName = args[2]};
                return true;
            }
            configuration = new DataExporterConfiguration() { DatabaseDataDir = args[0], OutputDumpPath = args[1]};
            return true;
        }

        private static void PrintUsage()
        {
            ConsoleWriteLineWithColor(ConsoleColor.DarkMagenta, @"
Data Export utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------", SystemTime.UtcNow.Year);

            Console.WriteLine(@"
Usage:
  - Export a database to a file in raven.dump format, use -ATTACHMENTS to include attachments in the dump file.
	Raven.DataExporter c:\RavenDB\Databases\Northwind\ c:\RavenDB\Dumps\Northwind\northwind.raven [-ATTACHMENTS]
  - Export a table from given database to a csv file.
	Raven.DataExporter c:\RavenDB\Databases\Northwind\ -T ref c:\RavenDB\Dumps\Northwind\ref.csv
 ");
            Console.WriteLine();
        }

        public static void ConsoleWriteLineWithColor(ConsoleColor color, string message, params object[] args)
        {
            var previousColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(message, args);
            Console.ForegroundColor = previousColor;
        }
    }
}
