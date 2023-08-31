using System.Runtime.InteropServices;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Voron;

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public struct PageHeaderUnion
{
    [FieldOffset(0)]
    public PageHeader PageHeader;

    [FieldOffset(0)]
    public FixedSizeTreePageHeader FixedSizeTreePageHeader;

    [FieldOffset(0)]
    public TreePageHeader TreePageHeader;
}
