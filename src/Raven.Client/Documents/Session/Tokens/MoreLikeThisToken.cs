using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class MoreLikeThisToken : WhereToken
    {
        public string DocumentParameterName;

        public string OptionsParameterName;

        public readonly LinkedList<QueryToken> WhereTokens;

        public MoreLikeThisToken()
        {
            WhereTokens = new LinkedList<QueryToken>();
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("moreLikeThis(");

            if (DocumentParameterName == null)
            {
                if (WhereTokens.Count > 0)
                {
                    var token = WhereTokens.First;
                    while (token != null)
                    {
                        DocumentQueryHelper.AddSpaceIfNeeded(token.Previous?.Value, token.Value, writer);

                        token.Value.WriteTo(writer);

                        token = token.Next;
                    }
                }
                else
                {
                    writer.Append("true");
                }
            }
            else
            {
                writer.Append("$");
                writer.Append(DocumentParameterName);
            }

            if (OptionsParameterName == null)
            {
                writer.Append(")");
                return;
            }

            writer.Append(", $");
            writer.Append(OptionsParameterName);
            writer.Append(")");
        }
    }
}
