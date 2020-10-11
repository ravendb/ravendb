using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class CloseSubclauseToken : QueryToken
    {
        public string BoostParameterName { get; set; }
        private CloseSubclauseToken()
        {
        }

        public static readonly CloseSubclauseToken Instance = new CloseSubclauseToken();

        public override void WriteTo(StringBuilder writer)
        {
            if (BoostParameterName != null)
                writer.Append(", $").Append(BoostParameterName);
            writer.Append(")");
        }
    }
}
