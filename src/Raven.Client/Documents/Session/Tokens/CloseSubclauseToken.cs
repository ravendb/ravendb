using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class CloseSubclauseToken : QueryToken
    {
        private CloseSubclauseToken()
        {
        }

        public static readonly CloseSubclauseToken Instance = new CloseSubclauseToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append(")");
        }
    }
}
