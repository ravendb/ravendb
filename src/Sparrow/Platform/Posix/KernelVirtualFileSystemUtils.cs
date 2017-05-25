using System;
using System.Collections.Generic;
using System.IO;
using Sparrow.Logging;

namespace Sparrow.Platform.Posix
{
    public static class KernelVirtualFileSystemUtils
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger(nameof(KernelVirtualFileSystemUtils), "Raven/Server");
        public static HashSet<string> IsOldFileAlert { get; set; }


        public static long ReadNumberFromCgroupFile(string filename)
        {
            // return long number read from file.  long.MaxValue is returned on error or on N/A value (-1)
            try
            {
                var txt = File.ReadAllText(filename);
                var result = Convert.ToInt64(txt);
                if (result < 0)
                    result = long.MaxValue;
                return result;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                {
                    Logger.Operations($"Unable to read and parse '{filename}', will not respect container's limit", e);
                }
                return long.MaxValue;
            }
        }

        public static int ReadNumberOfCoresFromCgroupFile(string filename)
        {
            // on error or if not applicable - return Environment.ProcessorCount
            try
            {
                var txt = File.ReadAllText(filename);
                // cpuset.cpus output format is a ranges of used core numbers comma seperated list. 
                // i.e. : 3,5-7,9 (which are 3,5,6,7,9 and the result should be 5 cores)
                // it is guaranteed to get the cores numbers and ranges from lower to upper
                var coresCount = 0;
                foreach (var coreRange in txt.Split(','))
                {
                    var cores = coreRange.Split('-');
                    if (cores.Length == 1)
                        coresCount++;
                    else
                        coresCount += Convert.ToInt32(cores[1]) - Convert.ToInt32(cores[0]) + 1;  // "5-7" == 3
                }
                return coresCount;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                {
                    Logger.Operations($"Unable to read and parse '{filename}', will not respect container's number of cores", e);
                }
                return Environment.ProcessorCount;
            }
        }

        public static string [] ReadSwapInformationFromSwapsFile()
        {
            // /proc/swaps output format :
            //              Filename        Type            Size            Used    Priority
            //              /dev/sda5       partition       16691196        109376  -1
            //                   .
            //                   .


            // on error or if not applicable - return null path array
            var filename = "/proc/swaps";
            try
            {
                var txt = File.ReadAllText(filename);
                var items = System.Text.RegularExpressions.Regex.Split(txt, @"\s+");

                if (items.Length < 6)
                {
                    if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"no swap defined on this system according to {filename}");
                    return null;
                }

                if (items[0].Equals("Filename") == false ||
                    items[1].Equals("Type") == false ||
                    items[2].Equals("Size") == false ||
                    items[3].Equals("Used") == false ||
                    items[4].Equals("Priority") == false)
                {
                    if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"Unrecognized header at {filename}, cannot read swap information");
                    return null;
                }

                if (items.Length % 5 != 0)
                {
                    if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"Invalid number of fields at {filename}, cannot read swap information");
                    return null;
                }

                var numberOfSwaps = items.Length / 5;
                var path = new string[numberOfSwaps];
              
                for (var i=5; i < numberOfSwaps; i+=5) // start from "5" - skip header
                {
                    path[i] = items[i];                    
                }

                return path;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                    Logger.Operations($"Unable to read and parse '{filename}', cannot read swap information", e);
                return null;
            }
        }

        public static long ReadNumberFromFile(string filename)
        {
            // return long number read from file, on error return -1
            try
            {
                var txt = File.ReadAllText(filename);
                var result = Convert.ToInt64(txt);
                return result;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                {
                    Logger.Operations($"Unable to read and parse '{filename}'", e);
                }
                return long.MaxValue;
            }
        }

        public static HashSet<string> GetAllDisksFromPartitionsFile()
        {
            // /proc/partitions output format :
            //          major minor  #blocks  name

            //          8        0  125033783 sda
            //          8        1  108340224 sda1
            //          8        2          1 sda2
            //          8        5   16691200 sda5
            //          8       16  488386584 sdb

            // on error or if not applicable - return empty hash set
            var filename = "/proc/partitions";
            var results = new HashSet<string>();
            try
            {
                var txt = File.ReadAllText(filename);
                var items = System.Text.RegularExpressions.Regex.Split(txt, @"\s+");

                if (items.Length < 5)
                {
                    if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"no partitions defined on this system according to {filename}");
                    return results;
                }

                if (items[0].Equals("major") == false ||
                    items[1].Equals("minor") == false ||
                    items[2].Equals("#blocks") == false ||
                    items[3].Equals("name") == false)
                {
                    if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"Unrecognized header at {filename}, cannot read partitions information");
                    return results;
                }

                if (items.Length % 4 != 0)
                {
                    if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                        Logger.Operations($"Invalid number of fields at {filename}, cannot read partitions information");
                    return results;
                }

                var numberOfSwaps = items.Length / 4;
                for (var i = 4; i < numberOfSwaps; i += 4) // start from "4" - skip header
                {
                    var reg = new System.Text.RegularExpressions.Regex(@"\d+$"); // remove numbers at end of string (i.e.: /dev/sda5 ==> /dev/sda)
                    var disk = reg.Replace(items[i], "");
                    results.Add(disk);
                }

                return results;
            }
            catch (Exception e)
            {
                if (IsOldFileAlert.Add(filename) && Logger.IsOperationsEnabled)
                    Logger.Operations($"Unable to read and parse '{filename}', cannot read partitions information", e);
                return new HashSet<string>();
            }
        }
    }
}
