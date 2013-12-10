// -----------------------------------------------------------------------
//  <copyright file="DiskFullException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;

namespace Voron.Exceptions
{
	public class DiskFullException: Exception
	{
		public DriveInfo DriveInfo { get; private set; }

		public DiskFullException(DriveInfo driveInfo)
			: base(string.Format("There is not enough space on the disk [Drive: {0}]", driveInfo.Name))
		{
			DriveInfo = driveInfo;
		}
	}
}