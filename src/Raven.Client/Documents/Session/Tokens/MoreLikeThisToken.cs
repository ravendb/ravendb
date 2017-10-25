using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class MoreLikeThisToken : QueryToken
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
                    foreach (var token in WhereTokens)
                        token.WriteTo(writer);
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
