using System;
using System.Runtime.InteropServices;

namespace Voron.Impl.FileHeaders;

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public struct MetadataFile
{
    /// <summary>
    /// Hash of the metadata used for validation
    /// </summary>
    [FieldOffset(0)]
    public ulong Hash;

    /// <summary>
    /// The version of the data, used for versioning / conflicts
    /// </summary>
    [FieldOffset(8)]
    public int Version;

    /// <summary>
    /// The journal id for all the transactions in shared journals
    /// for this environment that allows to tell which transactions
    /// belong to this environment or to others
    /// </summary>
    [FieldOffset(12)]
    public Guid JournalId;
}
