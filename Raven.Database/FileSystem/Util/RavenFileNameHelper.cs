using Raven.Abstractions.FileSystem;
using System;
using System.Globalization;
using System.Text;

namespace Raven.Database.FileSystem.Util
{
    public static class RavenFileNameHelper
    {
        public static string RavenDirectory(string directory)
        {
            directory = Uri.UnescapeDataString(directory);
            directory = directory.Replace("\\", "/");
            if (!directory.StartsWith("/"))
                directory = "/" + directory;

            return directory;
        }

        internal const string SyncNamePrefix = "Syncing";
        public static string SyncNameForFile(string fileName, string destination)
        {
            return SyncNamePrefix + "/" + Uri.EscapeUriString(destination) + FileHeader.Canonize(fileName);
        }

        internal const string SyncLockNamePrefix = "SyncingLock";
        public static string SyncLockNameForFile(string fileName)
        {
            return SyncLockNamePrefix + FileHeader.Canonize(fileName);
        }

        internal const string ConflictConfigNamePrefix = "Conflicted";
        public static string ConflictConfigNameForFile(string fileName)
        {
            return ConflictConfigNamePrefix + FileHeader.Canonize(fileName);
        }

        internal const string SyncResultNamePrefix = "SyncResult";
        public static string SyncResultNameForFile(string fileName)
        {
            return SyncResultNamePrefix + FileHeader.Canonize(fileName);
        }

        internal const string DownloadingFileSuffix = ".downloading";
        public static string DownloadingFileName(string fileName)
        {
            return FileHeader.Canonize(fileName) + DownloadingFileSuffix;
        }

        internal const string DeletingFileSuffix = ".deleting";
        public static string DeletingFileName(string fileName, int deleteVersion = 0)
        {
            return FileHeader.Canonize(fileName) + (deleteVersion > 0 ? deleteVersion.ToString(CultureInfo.InvariantCulture) : string.Empty) + DeletingFileSuffix;
        }

        internal const string DeleteOperationConfigPrefix = "DeleteOp";
        public static string DeleteOperationConfigNameForFile(string fileName)
        {
            return DeleteOperationConfigPrefix + FileHeader.Canonize(fileName);
        }

        internal const string RenameOperationConfigPrefix = "RenameOp";
        public static string RenameOperationConfigNameForFile(string fileName)
        {
            return RenameOperationConfigPrefix + FileHeader.Canonize(fileName);
        }

        internal const string CopyOperationConfigPrefix = "CopyOp";
        
        public static string CopyOperationConfigNameForFile(string fileName, string target)
        {
            return $"{CopyOperationConfigPrefix} {FileHeader.Canonize(fileName)} {target}";
        }
    }
}
