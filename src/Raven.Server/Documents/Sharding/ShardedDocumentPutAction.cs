using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding;

public class ShardedDocumentPutAction : DocumentPutAction
{
    public ShardedDocumentPutAction(DocumentsStorage documentsStorage, DocumentDatabase documentDatabase) : base(documentsStorage, documentDatabase)
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
}
