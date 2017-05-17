using System;
using System.IO;
using Sparrow.Logging;

namespace Sparrow.Platform.Posix
{
    public static class CgroupUtils
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger(nameof(CgroupUtils), "Raven/Server");

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
                if (Logger.IsInfoEnabled)
                    Logger.Info("Unable to read and parse '{filename}', will not resepect container's limit", e);
                return long.MaxValue;
            }
        }
    }
}
