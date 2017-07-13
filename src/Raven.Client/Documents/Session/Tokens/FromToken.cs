using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class FromToken : QueryToken
    {
        private bool? _isDynamic;

        public string IndexName { get; }

        public bool IsDynamic
        {
            get
            {
                if (_isDynamic.HasValue == false)
                    _isDynamic = IndexName.StartsWith(Constants.Documents.Querying.DynamicQueryPrefix, StringComparison.OrdinalIgnoreCase);

                return _isDynamic.Value;
            }
        }

        private FromToken(string indexName)
        {
            IndexName = indexName;
        }

        public static FromToken Create(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            return new FromToken(indexName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            if (IsDynamic)
            {
                writer
                    .Append("FROM ")
                    .Append(IndexName.Substring(Constants.Documents.Querying.DynamicQueryPrefix.Length));

                // TODO [ppekrol] add hints for auto-index
                return;
            }

            writer
                .Append("FROM INDEX '")
                .Append(IndexName)
                .Append("'");
        }
    }
}