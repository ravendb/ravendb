using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class NegateToken : QueryToken
    {
        private NegateToken()
        {
        }

        public static NegateToken Instance = new NegateToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("NOT");
        }

        public override QueryToken Clone()
        {
            return this;
        }
    }
}