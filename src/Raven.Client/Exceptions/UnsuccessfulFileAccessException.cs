using System;

namespace Raven.Client.Exceptions
{
    public class UnsuccessfulFileAccessException : Exception
    {
        private static string GetMessage(string filePath) => 
            $@"Failed to access a file at <{filePath}>. There are multiple possible reasons: {Environment.NewLine}
* RavenDB process has insufficient permissions to access the file. Please verify OS file permissions. {Environment.NewLine}
* The file is being used by some other process. {Environment.NewLine}
* The specified file path is a folder.
        ";

        public string FilePath { get; }

        public UnauthorizedAccessException OriginatingException { get; }

        public UnsuccessfulFileAccessException(UnauthorizedAccessException e, string filePath)
            : base(GetMessage(filePath),e)
        {
            OriginatingException = e;
            FilePath = filePath;            
        }
    }
}
