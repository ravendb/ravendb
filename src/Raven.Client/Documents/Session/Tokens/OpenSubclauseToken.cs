using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class OpenSubclauseToken : QueryToken
    {
        private OpenSubclauseToken()
        {
        }

        public string BoostParameterName { get; internal set; }

        internal static OpenSubclauseToken Create()
        {
            return new OpenSubclauseToken();
        }

        public override void WriteTo(StringBuilder writer)
        {
            if (BoostParameterName != null)
                writer.Append("boost");
            writer.Append("(");
        }
    }
}
