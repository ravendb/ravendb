using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class DistinctToken : QueryToken
    {
        private DistinctToken()
        {
        }

        public static readonly DistinctToken Instance = new DistinctToken();

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("distinct");
        }
    }
}
