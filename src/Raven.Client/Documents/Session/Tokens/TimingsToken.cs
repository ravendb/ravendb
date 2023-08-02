using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class TimingsToken : QueryToken
    {
        public static TimingsToken Instance = new TimingsToken();

        private TimingsToken()
        {
            
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("timings()");
        }
    }
}
