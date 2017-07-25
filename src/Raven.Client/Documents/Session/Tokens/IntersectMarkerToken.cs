using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class IntersectMarkerToken : QueryToken
    {
        private IntersectMarkerToken()
        {
        }

        public static IntersectMarkerToken Instance = new IntersectMarkerToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append(", ");
        }
    }
}