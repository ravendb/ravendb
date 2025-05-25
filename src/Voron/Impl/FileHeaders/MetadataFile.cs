using System;
using System.Runtime.InteropServices;

namespace Voron.Impl.FileHeaders;

[StructLayout(LayoutKind.Explicit, Pack = 1)]
public struct MetadataFile
{
    public static int HashOffset = (int)Marshal.OffsetOf<MetadataFile>(nameof(Hash));

    /// <summary>
    /// Current size of the metadata.
    /// Should allow more easily to extend this file and add more fields 
    /// </summary>
    [FieldOffset(0)]
    public int DataSize;

    /// <summary>
    /// Hash of the metadata used for validation
    /// </summary>
    [FieldOffset(4)]
    public ulong Hash;

    /// <summary>
    /// The version of the data, used for versioning / conflicts
    /// </summary>
    [FieldOffset(12)]
    public int Version;

    /// <summary>
    /// The journal id for all the transactions in shared journals
    /// for this environment that allows to tell which transactions
    /// belong to this environment or to others
    /// </summary>
    [FieldOffset(16)]
    public Guid JournalId;
}
