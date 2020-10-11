using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class OpenSubclauseToken : QueryToken
    {
        private OpenSubclauseToken()
        {
        }

        public static readonly OpenSubclauseToken Instance = new OpenSubclauseToken();
        public string BoostParameterName { get; set; }

        public override void WriteTo(StringBuilder writer)
        {
            if (BoostParameterName != null)
                writer.Append("boost");
            writer.Append("(");
        }
    }
}
