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

    public bool Initialize(bool isNewStore)
    {
        var hasMetadata = env.Options.ReadValidMetadata(MetadataName, out _metadata);
        if (hasMetadata == false)
        {
            if (isNewStore)
            {
                Modify(FillMetadata);
            }
            else
            {
                // A fresh id on an existing store would make recovery skip the store's own
                // journal transactions as foreign. Guid.Empty defers the choice to recovery,
                // which resolves the id from the journals before it applies anything.
                Modify(static (ref MetadataFile metadata) =>
                {
                    metadata.JournalId = Guid.Empty;
                    metadata.Version = Constants.CurrentVersion;
                });
            }
            return true;
        }

        return false;
    }

    public void FillMetadata(ref MetadataFile metadata)
    {
        metadata.JournalId = Guid.NewGuid();
        metadata.Version = Constants.CurrentVersion;
    }

    public void Modify(ModifyMetadataAction modifyAction)
    {
        modifyAction?.Invoke(ref _metadata);

        var buffer = MemoryMarshal.AsBytes(new Span<MetadataFile>(ref _metadata));
        _metadata.Hash = Hashing.XXHash64.CalculateInline(buffer[sizeof(ulong)..]);
        env.Options.WriteMetadata(MetadataName, _metadata);
    }
}
