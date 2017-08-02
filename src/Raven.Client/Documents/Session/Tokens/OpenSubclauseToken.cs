using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class OpenSubclauseToken : QueryToken
    {
        private OpenSubclauseToken()
        {
        }

        public static OpenSubclauseToken Instance = new OpenSubclauseToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("(");
        }
    }
}