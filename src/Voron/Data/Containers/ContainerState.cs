using System.Runtime.InteropServices;

namespace Voron.Data.Containers
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct ContainerState
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;
        [FieldOffset(8)]
        public long RootPage;
        [FieldOffset(16)]
        public long NumberOfPages;
        [FieldOffset(24)]
        public long NumberOfOverfowPages;
        [FieldOffset(32)]
        public long NumberOfEntries;
        [FieldOffset(40)]
        public long FreeListPage;
        [FieldOffset(48)]
        public long AllPagesListPage;

    }
}
