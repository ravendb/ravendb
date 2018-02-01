// -----------------------------------------------------------------------
//  <copyright file="DiskFullException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;

namespace Voron.Exceptions
{
    public class DiskFullException : Exception
    {
        public DiskFullException()
        {
        }

        public string DirectoryPath;
        public long CurrentFreeSpace;

        public DiskFullException(string filePath, long requestedFileSize, long? freeSpace)
            : base(
                $"There is not enough space for file {filePath} to {requestedFileSize / 1024:N1} KB. " +
                $"Currently available space: {(freeSpace / 1024) ?? -1:N1} KB"
            )
        {
            DirectoryPath = Path.GetDirectoryName(filePath);
            CurrentFreeSpace = freeSpace ?? requestedFileSize - 1;
        }
    }
}
