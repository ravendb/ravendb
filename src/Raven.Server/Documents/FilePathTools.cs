using System;
using System.IO;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Platform;

namespace Raven.Server.Documents
{
    public static class FilePathTools
    {
        public static string MakeSureEndsWithSlash(string filePath)
        {
            if (PlatformDetails.RunningOnPosix == false)
                return filePath.TrimEnd('\\') + "\\";
            return filePath.TrimEnd('\\').TrimEnd('/') + "/";
        }

        public static string StripWorkingDirectory(string workDir, string dir)
        {
            if (dir.StartsWith(workDir, StringComparison.OrdinalIgnoreCase))
                return "\t\\" + dir.Substring(workDir.Length);
            return dir;
        }
    }
}
