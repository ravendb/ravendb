using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class FromToken : QueryToken
    {
        public string CollectionName { get; }

        public string IndexName { get; }

        public bool IsDynamic { get; }

        private FromToken(string indexName, string collectionName)
        {
            CollectionName = collectionName;
            IndexName = indexName;
            IsDynamic = CollectionName != null;
        }

        public static FromToken Create(string indexName, string collectionName)
        {
            return new FromToken(indexName, collectionName);
        }

        private static readonly char[] _whiteSpaceChars = { ' ', '\t', '\r', '\n', '\v' };
        public override void WriteTo(StringBuilder writer)
        {
            if (IndexName == null && CollectionName == null)
                throw new NotSupportedException("Either IndexName or CollectionName must be specified");

            if (IsDynamic)
            {
                writer
                    .Append("FROM ");
                if(CollectionName.IndexOfAny(_whiteSpaceChars) != -1)
                {
                    if (CollectionName.IndexOf('"') != -1)
                    {
                        ThrowInvalidcollectionName();
                    }
                    writer.Append('"').Append(CollectionName).Append('"');
                }
                else
                {
                   WriteField(writer, CollectionName);
                }

                return;
            }

            writer
                .Append("FROM INDEX '")
                .Append(IndexName)
                .Append("'");
        }

        private void ThrowInvalidcollectionName()
        {
            throw new ArgumentException("Collection name cannot contain a quote, but was: " + CollectionName);
        }
    }
}
