using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class TrueToken : QueryToken
    {
        private TrueToken()
        {
        }

        public static readonly TrueToken Instance = new TrueToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("true");
        }
    }
}
