using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class CloseSubclauseToken : QueryToken
    {
        private CloseSubclauseToken()
        {
        }

        public static CloseSubclauseToken Instance = new CloseSubclauseToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append(")");
        }
    }
}