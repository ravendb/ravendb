using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class CloseSubclauseToken : QueryToken
    {
        private CloseSubclauseToken()
        {
        }

        public string BoostParameterName { get; internal set; }

        internal static CloseSubclauseToken Create()
        {
            return new CloseSubclauseToken();
        }

        public override void WriteTo(StringBuilder writer)
        {
            if (BoostParameterName != null)
                writer.Append(", $").Append(BoostParameterName);
            writer.Append(")");
        }
    }
}
