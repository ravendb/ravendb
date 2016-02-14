using System.Collections.Generic;

namespace Raven.Server.Indexing.Corax.Analyzers.Filters
{
    public class StopWordsFilter : IFilter
    {
        private static readonly string[] defaultStopWords =
        {
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", 
            "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"
        };

        private readonly HashSet<ArraySegmentKey<char>> _stopWords = new HashSet<ArraySegmentKey<char>>();

        public StopWordsFilter()
            : this(defaultStopWords)
        {
        }

        private StopWordsFilter(IEnumerable<string> stopWords)
        {
            foreach (var word in stopWords)
            {
                _stopWords.Add(new ArraySegmentKey<char>(word.ToCharArray()));
            }
        }

        public bool ProcessTerm(ITokenSource source)
        {
            var term = new ArraySegmentKey<char>(source.Buffer, source.Size);
            if (_stopWords.Contains(term))
                return false;
            return true;
        }
    }
}