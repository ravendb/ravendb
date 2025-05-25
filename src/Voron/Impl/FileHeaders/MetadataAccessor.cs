using System;
using System.Runtime.InteropServices;
using Sparrow;
using Voron.Global;

namespace Voron.Impl.FileHeaders;

public delegate void ModifyMetadataAction(ref MetadataFile header);

public sealed class MetadataAccessor(StorageEnvironment env)
{
    internal static string MetadataName = "metadata.one";

    private MetadataFile _metadata;
    public Guid JournalId => _metadata.JournalId;

    public bool Initialize()
    {
        var hasMetadata = env.Options.ReadValidMetadata(MetadataName, out _metadata);
        if (hasMetadata == false)
        {
            Modify(FillMetadata, persist: true);
            return true;
        }

        return false;
    }

    public unsafe void FillMetadata(ref MetadataFile metadata)
    {
        metadata.DataSize = sizeof(MetadataFile);
        metadata.JournalId = Guid.NewGuid();
        metadata.Version = Constants.CurrentVersion;

        var buffer = MemoryMarshal.AsBytes(new Span<MetadataFile>(ref metadata));
        metadata.Hash = Hashing.XXHash64.CalculateInline(buffer[(MetadataFile.HashOffset + sizeof(ulong))..]);
    }

    public void Modify(ModifyMetadataAction modifyAction, bool persist)
    {
        modifyAction?.Invoke(ref _metadata);

        var buffer = MemoryMarshal.AsBytes(new Span<MetadataFile>(ref _metadata));
        _metadata.Hash = Hashing.XXHash64.CalculateInline(buffer[(MetadataFile.HashOffset + sizeof(ulong))..]);
        if (persist)
            env.Options.WriteMetadata(MetadataName, _metadata);
    }
}
