using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class TrueToken : QueryToken
    {
        private TrueToken()
        {
        }

        public static TrueToken Instance = new TrueToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("true");
        }

        public override QueryToken Clone()
        {
            return this;
        }
    }
}