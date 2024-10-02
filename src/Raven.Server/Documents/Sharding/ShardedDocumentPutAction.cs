using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding;

public sealed class ShardedDocumentPutAction : DocumentPutAction
{
    public ShardedDocumentPutAction(ShardedDocumentsStorage documentsStorage, ShardedDocumentDatabase documentDatabase) : base(documentsStorage, documentDatabase)
    {
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
