using System;
using System.Diagnostics;
using System.Text;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents;

public static class CompoundKeyHelper
{
    public static string ExtractDocumentId(ReadOnlySpan<byte> key)
    {
        // key structure means doc-id | REC-SEP | ...
        int endOfDocumentId = key.IndexOf(SpecialChars.RecordSeparator);
        Debug.Assert(endOfDocumentId != -1);
        string documentId = Encoding.UTF8.GetString(key[..endOfDocumentId]);// will throw if no separator found
        return documentId;
    }
}
