using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public abstract class QueryToken
    {
        public abstract void WriteTo(StringBuilder writer);
    }
}