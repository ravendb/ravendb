using System;
using Lucene.Net.Documents;
using Raven.Abstractions.Indexing;

namespace Raven.Server.Documents.Indexes
{
    public static class IndexFieldExtensions
    {
        public static Field.TermVector GetLuceneValue(this FieldTermVector value)
        {
            switch (value)
            {
                case FieldTermVector.No:
                    return Field.TermVector.NO;
                case FieldTermVector.WithOffsets:
                    return Field.TermVector.WITH_OFFSETS;
                case FieldTermVector.WithPositions:
                    return Field.TermVector.WITH_POSITIONS;
                case FieldTermVector.WithPositionsAndOffsets:
                    return Field.TermVector.WITH_POSITIONS_OFFSETS;
                case FieldTermVector.Yes:
                    return Field.TermVector.YES;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public static Field.Index GetLuceneValue(this FieldIndexing value, string analyzer, Field.Index @default)
        {
            switch (value)
            {
                case FieldIndexing.No:
                    return Field.Index.NO;
                case FieldIndexing.Analyzed:
                    return string.IsNullOrWhiteSpace(analyzer) == false ? Field.Index.ANALYZED : Field.Index.ANALYZED_NO_NORMS;
                case FieldIndexing.NotAnalyzed:
                    return Field.Index.NOT_ANALYZED_NO_NORMS;
                case FieldIndexing.Default:
                    return @default;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public static Field.Store GetLuceneValue(this FieldStorage value)
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