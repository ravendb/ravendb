using System.Text;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    internal class DocumentQueryHelper
    {
        internal static void AddSpaceIfNeeded(QueryToken previousToken, QueryToken currentToken, StringBuilder writer)
        {
            if (previousToken == null)
                return;

            if (previousToken is OpenSubclauseToken || currentToken is CloseSubclauseToken || currentToken is IntersectMarkerToken)
                return;

            writer.Append(" ");
        }
    }
}
