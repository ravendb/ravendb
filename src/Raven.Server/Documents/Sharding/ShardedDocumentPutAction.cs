using System.Threading.Tasks;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding;

public sealed class ShardedDocumentPutAction : DocumentPutAction
{
    private readonly ShardedDocumentDatabase _documentDatabase;

    public ShardedDocumentPutAction(ShardedDocumentsStorage documentsStorage, ShardedDocumentDatabase documentDatabase) : base(documentsStorage, documentDatabase)
    {
        _documentDatabase = documentDatabase;
    }

    // TODO need to make sure we check that for counters/TS/etc...
    public override void ValidateId(DocumentsOperationContext context, Slice lowerId, DocumentFlags documentFlags = DocumentFlags.None)
    {
        if (documentFlags.Contain(DocumentFlags.FromResharding))
            return;

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Handle write documents to the wrong shard");
        var config = _documentDatabase.ShardingConfiguration;
        var bucket = ShardHelper.GetBucketFor(config, lowerId);
        var shard = ShardHelper.GetShardNumberFor(config, bucket);
        if (shard != _documentDatabase.ShardNumber)
        {
            if (_documentDatabase.ForTestingPurposes != null && _documentDatabase.ForTestingPurposes.EnableWritesToTheWrongShard)
                return;

            if (documentFlags.Contain(DocumentFlags.FromReplication))
            {
                // RavenDB-21104
                // we allow writing the document to the wrong shard to avoid inconsistent data within the shard group
                // and handle the leftovers at the end of the transaction 
                var task = new Task(delegate { _ = _documentDatabase.DocumentsMigrator.ExecuteMoveDocumentsAsync(); });
                context.Transaction.InvokeDocumentsMigration(task);
                return;
            }

            throw new ShardMismatchException($"Document '{lowerId}' belongs to bucket '{bucket}' on shard #{shard}, but PUT operation was performed on shard #{_documentDatabase.ShardNumber}.");
        }
    }

    protected override unsafe void CalculateSuffixForIdentityPartsSeparator(string id, ref char* idSuffixPtr, ref int idSuffixLength, ref int idLength)
    {
        if (id.Length < 2)
            return;

        var penultimateChar = id[^2];
        if (penultimateChar == '$')
        {
            idSuffixLength = id.Length - 2;

            ShardHelper.ExtractStickyId(ref idSuffixPtr, ref idSuffixLength);

            idSuffixLength += 1; // +1 for identity parts separator
            idLength -= idSuffixLength + 2; // +2 for 2x '$'
        }
    }

    protected override unsafe void WriteSuffixForIdentityPartsSeparator(ref char* valueWritePosition, char* idSuffixPtr, int idSuffixLength)
    {
        if (idSuffixLength <= 0) 
            return;

        valueWritePosition -= idSuffixLength;

        valueWritePosition[0] = '$';
        for (var j = 0; j < idSuffixLength - 1; j++)
            valueWritePosition[j + 1] = idSuffixPtr[j];
    }
}
