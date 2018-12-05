using System;
using System.IO;

namespace Raven.IndexCleaner
{
    class Program
    {
        static void Main(string[] args)
        {
            IndexCleaner cleaner;
            var valid = ValidateArgs(args, out cleaner);
            if (valid == false)
            {
                PrintUsage();
                Console.Read();
                return;
            }

            cleaner.Clean();
        }

        private static bool ValidateArgs(string[] args, out IndexCleaner indexCleaner)
        {
            indexCleaner = null;
            if (args.Length < 1)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Not enough arguments were provided.\n");
                return false;
            }
            if (Directory.Exists(args[0]) == false)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, $"The provided directory: {args[0]}, does not exsist.\n");
                return false;
            }
            try
            {
                indexCleaner = new IndexCleaner(args[0]);
            }
            catch (Exception e)
            {
                ConsoleUtils.PrintErrorAndFail($"Failed to init transactional storage, error message {e.Message}", e.StackTrace);
                return false;
            }

            return true;
        }

        private static void PrintUsage()
        {
            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.DarkMagenta, @"
Index cleaning utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------", DateTime.UtcNow.Year);

            Console.WriteLine(@"
Usage:
  - Clean all index related data from a specified RavenDB database.
    Raven.IndexCleaner.exe c:\RavenDB\Databases\Northwind\ 
 ");
            Console.WriteLine();
        }
    }
}
