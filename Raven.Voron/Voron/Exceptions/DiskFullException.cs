// -----------------------------------------------------------------------
//  <copyright file="DiskFullException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Runtime.Serialization;

namespace Voron.Exceptions
{
	[Serializable]
	public class DiskFullException : Exception
	{
		public DriveInfo DriveInfo { get; private set; }

		public DiskFullException()
		{
		}

		public DiskFullException(DriveInfo driveInfo, string filePath, long requestedFileSize)
			: base(
				string.Format("There is not enough space on {0} drive to set size of file {1} to {2} bytes", driveInfo.Name,
				              filePath, requestedFileSize))
		{
			DriveInfo = driveInfo;
		}

		protected DiskFullException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}