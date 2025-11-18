using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.BackgroundWork;

public abstract class DocumentBackgroundWorkStorage : AbstractBackgroundWorkStorage<DocumentExpirationInfo>
{
    protected DocumentBackgroundWorkStorage(Transaction tx, DocumentDatabase database, string treeName, string metadataPropertyName)
        : base(tx, database, treeName, metadataPropertyName)
    {
    }

    protected override DocumentExpirationInfo GetBackgroundWorkInfo(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        using var document = Database.DocumentsStorage.Get(options.Context, clonedId, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
        if (document == null ||
            document.TryGetMetadata(out var metadata) == false ||
            HasPassed(metadata, options.CurrentTime, MetadataPropertyName) == false)
        {
            return new DocumentExpirationInfo(ticksSlice, clonedId, null, BackgroundWorkInfoStatus.Delete);
        }

        return new DocumentExpirationInfo(ticksSlice, clonedId, document.Id, BackgroundWorkInfoStatus.Process);
    }
}
