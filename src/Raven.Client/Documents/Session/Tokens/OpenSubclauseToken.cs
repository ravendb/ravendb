using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class OpenSubclauseToken : QueryToken
    {
        private OpenSubclauseToken()
        {
        }

        public static readonly OpenSubclauseToken Instance = new OpenSubclauseToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("(");
        }
    }
}
