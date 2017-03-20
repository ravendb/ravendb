// -----------------------------------------------------------------------
//  <copyright file="EndOfDiskSpaceEvent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Runtime.ExceptionServices;

namespace Voron.Util
{
    public class EndOfDiskSpaceEvent
    {
        private readonly long _availableSpaceWhenEventOccurred;
        private readonly DriveInfo _driveInfo;
        private readonly ExceptionDispatchInfo _edi;

        public EndOfDiskSpaceEvent(DriveInfo driveInfo, ExceptionDispatchInfo edi)
        {
            _availableSpaceWhenEventOccurred = driveInfo.AvailableFreeSpace;
            _driveInfo = driveInfo;
            _edi = edi;
        }

        public void AssertCanContinueWriting()
        {
            if (_driveInfo.AvailableFreeSpace > _availableSpaceWhenEventOccurred)
                return;
            _edi.Throw();
        }
    }
}
