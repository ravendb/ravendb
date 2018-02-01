// -----------------------------------------------------------------------
//  <copyright file="EndOfDiskSpaceEvent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Runtime.ExceptionServices;
using Voron.Platform.Win32;

namespace Voron.Util
{
    public class EndOfDiskSpaceEvent
    {
        private readonly long _availableSpaceWhenEventOccurred;
        private readonly string _path;
        private readonly ExceptionDispatchInfo _edi;

        public EndOfDiskSpaceEvent(string path, long availableSpaceWhenEventOccurred, ExceptionDispatchInfo edi)
        {
            _availableSpaceWhenEventOccurred = availableSpaceWhenEventOccurred;
            _path = path;
            _edi = edi;
        }

        private static long GetAvailableFreeSpace(string path)
        {
            if(StorageEnvironmentOptions.RunningOnPosix == false)
            {
                if(Win32NativeFileMethods.GetDiskFreeSpaceEx(path, out _, out _, out var total) == false)
                {
                    return 0;
                }
                return (long)total;
            }

            return new DriveInfo(path).AvailableFreeSpace;
        }

        public void AssertCanContinueWriting()
        {
            if (GetAvailableFreeSpace(_path) > _availableSpaceWhenEventOccurred)
                return;
            _edi.Throw();
        }
    }
}
