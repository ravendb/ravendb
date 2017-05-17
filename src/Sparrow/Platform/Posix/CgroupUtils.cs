using System;
using System.IO;
using Sparrow.Logging;

namespace Sparrow.Platform.Posix
{
    public static class CgroupUtils
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger(nameof(CgroupUtils), "Raven/Server");
        private static bool IsOldLimitAlert { get; set; }
        private static bool IsOldCoresAlert { get; set; }

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
                if (IsOldLimitAlert == false && Logger.IsOperationsEnabled)
                {
                    IsOldLimitAlert = true;
                    Logger.Operations("Unable to read and parse '{filename}', will not resepect container's limit", e);
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
                // it is guarenteed to get the cores numbers and ranges from lower to upper
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
                if (IsOldCoresAlert == false && Logger.IsOperationsEnabled)
                {
                    IsOldCoresAlert = true;
                    Logger.Operations("Unable to read and parse '{filename}', will not resepect container's number of cores", e);
                }
                return Environment.ProcessorCount;
            }
        }
    }
}
