// -----------------------------------------------------------------------
//  <copyright file="EndOfDiskSpaceEvent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;

namespace Voron.Util
{
	public class EndOfDiskSpaceEvent
	{
		private readonly long _availableSpaceWhenEventOccurred;
		private DriveInfo _driveInfo;

		public EndOfDiskSpaceEvent(DriveInfo driveInfo)
		{
			_availableSpaceWhenEventOccurred = driveInfo.AvailableFreeSpace;
			_driveInfo = driveInfo;
		}

		public bool CanContinueWriting
		{
			get { return _driveInfo.AvailableFreeSpace > _availableSpaceWhenEventOccurred; }
		}
	}
}