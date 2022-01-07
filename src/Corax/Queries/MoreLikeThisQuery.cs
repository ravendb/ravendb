using System;
using System.Collections.Generic;
using System.Text;
using Corax.Pipeline;
using Sparrow.Json;

namespace Corax.Queries
{
    public class MoreLikeThisQuery 
    {
        public static IQueryMatch Build(IndexSearcher searcher, BlittableJsonReaderObject doc, Analyzer analyzer)
        {
            IQueryMatch match = TermMatch.CreateEmpty();
            BlittableJsonReaderObject.PropertyDetails prop = default;
            for (int i = 0; i < doc.Count; i++)
            {
                doc.GetPropertyByIndex(i, ref prop);
                switch (prop.Token & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Integer:
                    case BlittableJsonToken.LazyNumber:
                    case BlittableJsonToken.Boolean:
                        match = searcher.Or(match, searcher.TermQuery(prop.Name, prop.Value.ToString()));
                        break;
                    case BlittableJsonToken.String:
                        match = searcher.Or(match, CreateAnalyzedQuery(prop.Name, searcher, (LazyStringValue)prop.Value, analyzer));
                        break;
                    case BlittableJsonToken.CompressedString:
                        match = searcher.Or(match, CreateAnalyzedQuery(prop.Name, searcher, ((LazyCompressedStringValue)prop.Value).ToLazyStringValue(), analyzer));
                        break;
                }
            }
            return BoostingMatch.WithTermFrequency(searcher, match);
        }
        
        private static IQueryMatch CreateAnalyzedQuery(string field, IndexSearcher searcher, LazyStringValue value, Analyzer analyzer)
        {
            if (analyzer == null)
            {
                return searcher.TermQuery(field, value);
            }
            analyzer.GetOutputBuffersSize(IndexWriter.MaxTermLength, out int bufferSize, out int tokenSize);
            Span<byte> tempWordsSpace = stackalloc byte[bufferSize];
            Span<Token> tempTokenSpace = stackalloc Token[tokenSize];
            analyzer.Execute(value.AsSpan(), ref tempWordsSpace, ref tempTokenSpace);
            var terms = new List<string>();
            for (int i = 0; i < tempTokenSpace.Length; i++)
            {
                terms.Add(Encoding.UTF8.GetString(tempWordsSpace.Slice(tempTokenSpace[i].Offset, (int)tempTokenSpace[i].Length)));
            }
            return searcher.InQuery(field, terms);
        }
    }
}
