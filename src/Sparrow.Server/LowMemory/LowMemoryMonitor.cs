﻿using System.Buffers;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Sparrow.Server.LowMemory
{
    internal sealed class LowMemoryMonitor : AbstractLowMemoryMonitor
    {
        private readonly SmapsReader _smapsReader;

        private byte[][] _buffers;

        public LowMemoryMonitor()
        {
            if (PlatformDetails.RunningOnLinux)
            {
                var buffer1 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                var buffer2 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                _buffers = new[] { buffer1, buffer2 };
                _smapsReader = new SmapsReader(_buffers);
            }
        }

        public override MemoryInfoResult GetMemoryInfoOnce()
        {
            return MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();
        }

        public override MemoryInfoResult GetMemoryInfo(bool extended = false)
        {
            return MemoryInformation.GetMemoryInfo(extended ? _smapsReader : null, extended: extended);
        }

        public override bool IsEarlyOutOfMemory(MemoryInfoResult memInfo, out Size commitChargeThreshold)
        {
            return MemoryInformation.IsEarlyOutOfMemory(memInfo, out commitChargeThreshold);
        }

        public override DirtyMemoryState GetDirtyMemoryState()
        {
            return MemoryInformation.GetDirtyMemoryState();
        }

        public override void AssertNotAboutToRunOutOfMemory()
        {
            MemoryInformation.AssertNotAboutToRunOutOfMemory();
        }

        public override void Dispose()
        {
            if (_buffers != null)
            {
                ArrayPool<byte>.Shared.Return(_buffers[0]);
                ArrayPool<byte>.Shared.Return(_buffers[1]);

                _buffers = null;
            }
        }
    }
}
