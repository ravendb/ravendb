using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Utils;

namespace Raven.Server.Utils
{
    public static class IndexingUtils
    {
        public static void ThrowDiskFullException(string path) // Can be the folder path of the fole absolute path
        {
            var folderPath = Path.GetDirectoryName(path); // file Absolute Path
            var driveInfo = DiskUtils.GetDiskSpaceInfo(folderPath);
            var freeSpace = driveInfo != null ? driveInfo.TotalFreeSpace.ToString() : "N/A";
            throw new DiskFullException($"There isn't enough space to flush the buffer in: {folderPath}. " +
                                        $"Currently available space: {freeSpace}");
        }
    }
}
