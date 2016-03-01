using System;
using Lucene.Net.Documents;
using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Indexes
{
    public static class IndexFieldExtensions
    {
        public static Field.Index GetLuceneValue(this FieldIndexing value, Field.Index? @default)
        {
            switch (value)
            {
                case FieldIndexing.No:
                    return Field.Index.NO;
                case FieldIndexing.Analyzed:
                    return Field.Index.ANALYZED_NO_NORMS;
                case FieldIndexing.NotAnalyzed:
                    return Field.Index.NOT_ANALYZED_NO_NORMS;
                case FieldIndexing.Default:
                    return @default ?? Field.Index.ANALYZED_NO_NORMS;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public static Field.Store GetLuceneValue(this FieldStorage value, Field.Store @default)
        {
            switch (value)
            {
                case FieldStorage.Yes:
                    return Field.Store.YES;
                case FieldStorage.No:
                    return Field.Store.NO;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}