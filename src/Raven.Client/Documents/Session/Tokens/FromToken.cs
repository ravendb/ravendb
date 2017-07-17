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

        public override void WriteTo(StringBuilder writer)
        {
            if (IsDynamic)
            {
                writer
                    .Append("FROM ")
                    .Append(CollectionName);

                return;
            }

            writer
                .Append("FROM INDEX '")
                .Append(IndexName)
                .Append("'");
        }
    }
}