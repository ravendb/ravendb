using System;
using System.Runtime.InteropServices;

namespace Raven.Database.FileSystem.Synchronization.Rdc.Wrapper.Unmanaged
{
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    internal struct RdcSignature
    {
        public IntPtr Signature;
        public ushort BlockLength;
    }
}
