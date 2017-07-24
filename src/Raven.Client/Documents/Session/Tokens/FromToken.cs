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
            if (indexName == null && collectionName == null)
                throw new NotSupportedException();

            if (indexName != null && collectionName != null)
                throw new NotSupportedException();

            return new FromToken(indexName, collectionName);
        }

        private static char[] _whiteSpaceChars = new[] { ' ', '\t', '\r', '\n', '\v' };
        public override void WriteTo(StringBuilder writer)
        {
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
                    writer.Append(CollectionName);
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