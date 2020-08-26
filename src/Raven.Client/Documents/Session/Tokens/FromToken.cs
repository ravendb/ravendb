using System;
using System.Text;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    public class FromToken : QueryToken
    {
        public string CollectionName { get; }

        public string IndexName { get; }

        public bool IsDynamic { get; }

        public string Alias { get; }

        private FromToken(string indexName, string collectionName, string alias = null)
        {
            CollectionName = collectionName;
            IndexName = indexName;
            IsDynamic = CollectionName != null;
            Alias = alias;
        }

        public static FromToken Create(string indexName, string collectionName, string alias = null)
        {
            return new FromToken(indexName, collectionName, alias);
        }

        public override void WriteTo(StringBuilder writer)
        {
            if (IndexName == null && CollectionName == null)
                throw new NotSupportedException("Either IndexName or CollectionName must be specified");

            if (IsDynamic)
            {
                writer
                    .Append("from '")
                    .Append(StringExtensions.EscapeString(CollectionName))
                    .Append("'");
            }
            else
            {
                writer
                    .Append("from index '")
                    .Append(IndexName)
                    .Append("'");
            }

            if (Alias != null)
            {
                writer.Append(" as ").Append(Alias);
            }
        }
    }
}
