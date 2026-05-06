using Voron.Impl.FileHeaders;

namespace Voron.Schema.Updates
{
    public sealed class From23 : IVoronSchemaUpdate
    {
        public bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade)
        {
            // Version 24 adds support for inline streams in trees.
            // Small streams (attachments) that fit within a tree node's max value size
            // are stored inline instead of using separate overflow pages + FixedSizeTree.
            // Backward-compatible with existing v23 data: existing chunked streams continue to work as before.
            // One-way upgrade: once v24 inline streams are written, older binaries cannot read them.
            // New inline streams use RootObjectType.InlineStream to distinguish them from FixedSizeTree-based storage.
            // No data migration is needed - existing streams continue to work unchanged on upgrade.

            versionAfterUpgrade = 24;

            return true;
        }
    }
}
