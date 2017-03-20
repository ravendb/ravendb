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
        public DriveInfo DriveInfo { get; private set; }

        public DiskFullException()
        {
        }

        public DiskFullException(DriveInfo driveInfo, string filePath, long requestedFileSize)
            : base(
                $"There is not enough space on {driveInfo?.Name ?? "unknown"} drive to set size of file {filePath} to {requestedFileSize / 1024:N1} KB. " +
                $"Currently available space: {(driveInfo?.AvailableFreeSpace / 1024) ?? -1:N1} KB"
            )
        {
            DriveInfo = driveInfo;
        }
    }
}
