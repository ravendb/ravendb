using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class FalseToken : QueryToken
    {
        private FalseToken()
        {
        }

        public static readonly FalseToken Instance = new FalseToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("false");
        }
    }
}
