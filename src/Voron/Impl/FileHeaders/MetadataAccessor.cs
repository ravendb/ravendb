using System;
using System.Runtime.InteropServices;
using Sparrow;
using Voron.Global;

namespace Voron.Impl.FileHeaders;

public delegate void ModifyMetadataAction(ref MetadataFile header);

public sealed class MetadataAccessor(StorageEnvironment env)
{
    internal static string MetadataName = "database.metadata";

    private MetadataFile _metadata;
    public Guid JournalId => _metadata.JournalId;

    public bool Initialize()
    {
        var hasMetadata = env.Options.ReadValidMetadata(MetadataName, out _metadata);
        if (hasMetadata == false)
        {
            Modify(FillMetadata);
            return true;
        }

        return false;
    }

    public void FillMetadata(ref MetadataFile metadata)
    {
        metadata.JournalId = Guid.NewGuid();
        metadata.Version = Constants.CurrentVersion;
        _metadata = metadata;
    }

    public void Modify(ModifyMetadataAction modifyAction)
    {
        modifyAction?.Invoke(ref _metadata);

        var buffer = MemoryMarshal.AsBytes(new Span<MetadataFile>(ref _metadata));
        _metadata.Hash = Hashing.XXHash64.CalculateInline(buffer[sizeof(ulong)..]);
        env.Options.WriteMetadata(MetadataName, _metadata);
    }
}
