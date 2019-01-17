using System;

namespace Voron.Exceptions
{
    public class IncreasingDataFileInCopyOnWriteModeException : Exception
    {
        public IncreasingDataFileInCopyOnWriteModeException(string dataFilePath, long requestedSize)
        {
            DataFilePath = dataFilePath;
            RequestedSize = requestedSize;
        }

        public string DataFilePath { get; }

        public long RequestedSize { get; }
    }
}
