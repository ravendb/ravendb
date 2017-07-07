using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class DistinctToken : QueryToken
    {
        private DistinctToken()
        {
        }

        public static DistinctToken Instance = new DistinctToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("DISTINCT");
        }

        public override QueryToken Clone()
        {
            return this;
        }
    }
}