using System;
using System.Diagnostics.CodeAnalysis;
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
            return new DocumentExpirationInfo(ticksSlice, clonedId, null, DocumentExpirationInfoStatus.Delete);
        }

        return new DocumentExpirationInfo(ticksSlice, clonedId, document.Id, DocumentExpirationInfoStatus.Process);
    }

    [DoesNotReturn]
    protected override void ThrowWrongDateFormat(Slice treeKey, string expirationDate)
    {
        throw new InvalidOperationException(
            $"The due date format for document '{treeKey}' is not valid: '{expirationDate}'. Use the following format: {Database.Time.GetUtcNow():O}");
    }
}
