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
            // This is a forward-compatible change: existing data remains valid,
            // new inline streams use RootObjectType.InlineStream to distinguish them
            // from the FixedSizeTree-based storage.
            // No data migration is needed - existing streams continue to work as before.

            versionAfterUpgrade = 24;

            return true;
        }
    }
}
