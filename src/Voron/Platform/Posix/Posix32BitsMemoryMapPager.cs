using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform.Posix;
using Sparrow.Utils;
using Voron.Data;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Platform.Win32;

namespace Voron.Platform.Posix
{
    public unsafe class Posix32BitsMemoryMapPager : PosixAbstractPager
    {
        public Posix32BitsMemoryMapPager(StorageEnvironmentOptions options, string file, long? initialFileSize = null,
                    bool usePageProtection = false) : base(options, usePageProtection)
        {
        }

        public override unsafe long TotalAllocationSize { get; }
        protected override unsafe string GetSourceName()
        {
            throw new NotImplementedException();
        }

        public override unsafe void Sync(long totoalUnsynced)
        {
            throw new NotImplementedException();
        }

        protected override unsafe PagerState AllocateMorePages(long newLength)
        {
            throw new NotImplementedException();
        }

        public override unsafe string ToString()
        {
            throw new NotImplementedException();
        }

        public override unsafe void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            throw new NotImplementedException();
        }
    }
}
