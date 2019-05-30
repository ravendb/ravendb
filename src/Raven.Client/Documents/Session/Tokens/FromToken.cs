using System;
using System.Text;

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

        private static readonly char[] _whiteSpaceChars = { ' ', '\t', '\r', '\n', '\v' };
        public override void WriteTo(StringBuilder writer)
        {
            if (IndexName == null && CollectionName == null)
                throw new NotSupportedException("Either IndexName or CollectionName must be specified");

            if (IsDynamic)
            {
                writer
                    .Append("from ");
                if (CollectionName.IndexOfAny(_whiteSpaceChars) != -1)
                {
                    if (CollectionName.IndexOf('"') != -1)
                    {
                        ThrowInvalidCollectionName();
                    }
                    writer.Append('"').Append(CollectionName).Append('"');
                }
                else
                {
                   WriteField(writer, CollectionName);
                }
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

        private void ThrowInvalidCollectionName()
        {
            throw new ArgumentException("Collection name cannot contain a quote, but was: " + CollectionName);
        }
    }
}
