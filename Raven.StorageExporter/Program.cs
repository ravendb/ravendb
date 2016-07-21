using System;
using System.IO;
using System.Linq;
using System.Security;
using System.Security.Permissions;
using Raven.Abstractions;
using Raven.Abstractions.Data;

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
            if (Directory.Exists(args[0]) == false)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Directory {0} does not exists.\n",args[0]);                
                return false; 
            }
            if (StorageExporter.ValidateStorageExists(args[0]) == false)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Directory {0} is not a valid RavenDB storage directory.\n", args[0]);
                return false; 
            }
            var outputDirectory = Path.GetDirectoryName(args[1]);
            if (Directory.Exists(outputDirectory) == false)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Output directory {0} does not exists.\n", outputDirectory);
                return false;
            }

            var permissionSet = new PermissionSet(PermissionState.None);
            var writePermission = new FileIOPermission(FileIOPermissionAccess.Write, args[1]);
            permissionSet.AddPermission(writePermission);

            if (permissionSet.IsSubsetOf(AppDomain.CurrentDomain.PermissionSet) == false)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "You don't have premissions to write to {0}.\n", args[1]);
            }
                    
            var currArgPos = 2;
            configuration = new StorgaeExporterConfiguration {DatabaseDataDir = args[0], OutputDumpPath = args[1]};
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
                    case "-DocumentsStartEtag":
                        Etag etag;
                        if (Etag.TryParse(args[currArgPos + 1], out etag) == false)
                        {
                            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "DocumentsStartEtag should be in a valid Etag format, we got {0}.\n", args[currArgPos + 1]);
                            return false;
                        }
                        configuration.DocumentsStartEtag = etag;
                        currArgPos += 2;
                        break;
                    case "--Compression":
                        configuration.HasCompression = true;
                        currArgPos += 1;
                        break;
                    case "-Encryption":
                        if (args.Length - currArgPos < 3)
                        {
                            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Not enough parameters for encryption");
                            return false;
                        }

                        var encryption = new EncryptionConfiguration();

                        var encryptionKey = args[currArgPos + 1];
                        if (encryption.TrySavingEncryptionKey(encryptionKey) == false)
                        {
                            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Encryption key should be in base64 string format, we got {0}.\n", args[currArgPos + 1]);
                            return false;
                        }

                        string algorithmType = args[currArgPos + 2];
                        if (encryption.TrySavingAlgorithmType(algorithmType) == false)
                        {
                            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Unknown encryption algorithm type, we got {0}.\n", args[currArgPos + 2]);
                            return false;
                        }

                        string preferedEncryptionKeyBitsSize = args[currArgPos + 3];
                        if (encryption.SavePreferedEncryptionKeyBitsSize(preferedEncryptionKeyBitsSize) == false)
                        {
                            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Encryption key bit size should be in an int, we got {0}.\n", args[currArgPos + 3]);
                            return false;
                        }

                        configuration.Encryption = encryption;
                        currArgPos += 4;
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
    Raven.StorageExporter.exe c:\RavenDB\Databases\Northwind\ c:\RavenDB\Dumps\Northwind\northwind.raven [-BatchSize]
  - Export an ESENT table from given RavenDB database into a CSV formated file.
    Raven.StorageExporter.exe c:\RavenDB\Databases\Northwind\ c:\RavenDB\Dumps\Northwind\ref.csv -T ref 

Parameters:
 -T <TableName> : The name of the table to be exported.
 -BatchSize <integer number> : The size of the export batch (default size is 1024). 
 -DocumentsStartEtag <Etag> : The document etag to start the export from (default is Etag.Empty).
 --Compression : Indicates that the database has the compression bundle.
 -Encryption <Encryption Key> <Algorithm Type> <Prefered Encryption Key Bits Size>: Encryption bundle key, algorithm type and bits size.
 ");
            Console.WriteLine();
        }
    }
}
