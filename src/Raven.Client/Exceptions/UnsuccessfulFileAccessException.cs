using System;
using System.IO;
using System.Security;
using Raven.Client.Extensions;
using Sparrow.Platform;

namespace Raven.Client.Exceptions
{
    internal class UnsuccessfulFileAccessException : Exception
    {
        internal static string GetMessage(string filePath, FileAccess accessType, Exception underlyingException)
        {
            var msg = $@"Failed to get {accessType} access a file at <{filePath}>.{Environment.NewLine}Possible reasons:{Environment.NewLine}";

            switch (underlyingException)
            {
                //precaution, for completeness sake
                case ArgumentException _: //for windows
                case NotSupportedException _: //for posix
                    msg += $"* The file path refers to a non-file device.{Environment.NewLine}";
                    break;

                case PathTooLongException _: //precaution, for completeness sake
                    msg += $"* The specified path, file name, or both exceed the system-defined maximum length.{Environment.NewLine}";
                    break;

                case FileNotFoundException _:
                    msg += $"* The file path refers to a non-existing file.{Environment.NewLine}";
                    break;

                case SecurityException _:
                    msg += $"* RavenDB process does not have the required permissions to open the file.{Environment.NewLine}";
                    break;
            }

            if ((File.GetAttributes(filePath) & FileAttributes.Directory) != 0)
            {
                msg += $@"* The file path points to a directory and thus cannot be accessed as a file. {Environment.NewLine}";
            }

            if (PlatformDetails.RunningOnPosix)
            {
                msg +=
                    $"* The file may be locked by another process that has it opened. The 'lslk' utility can help list locking processes. Please refer to man pages for more information{Environment.NewLine}";
            }
            else
            {
                try
                {
                    var whoIsLocking = WhoIsLocking.GetProcessesUsingFile(filePath);
                    if (whoIsLocking.Count > 0) //on posix this will be always empty
                    {
                        msg += $@"* The file is being used by the following process(es): {string.Join(",", whoIsLocking)}. {Environment.NewLine}";
                    }
                }
                catch (Exception e)
                {
                    msg += $@"* Failed to identify which process(es) is holding the file: {e}. {Environment.NewLine}";
                }
            }

            return msg;
        }

        public string FilePath { get; }
        public FileAccess AccessType { get; }

        public UnsuccessfulFileAccessException(Exception e, string filePath, FileAccess accessType)
            : base(GetMessage(filePath, accessType, e), e)
        {
            FilePath = filePath;
            AccessType = accessType;
        }
    }
}
