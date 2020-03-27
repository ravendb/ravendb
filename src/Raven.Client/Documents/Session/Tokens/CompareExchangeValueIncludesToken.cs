using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class CompareExchangeValueIncludesToken : QueryToken
    {
        private readonly string _path;

        private CompareExchangeValueIncludesToken(string path)
        {
            if (path is null)
                throw new ArgumentNullException(nameof(path));
            _path = path;
        }

        internal static CompareExchangeValueIncludesToken Create(string path)
        {
            return new CompareExchangeValueIncludesToken(path);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("cmpxchg('")
                .Append(_path)
                .Append("')");
        }
    }
}
