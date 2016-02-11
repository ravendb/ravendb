using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Json;

namespace Tryouts.Corax.Analyzers
{
    public class StopWordsFilter : IFilter
    {
        private static readonly string[] DefaultStopWords =
        {
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
            "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"
        };
        private readonly HashSet<object> _stopWords = new HashSet<object>();

        public StopWordsFilter()
            : this(DefaultStopWords)
        {
        }

        private StopWordsFilter(IEnumerable<string> stopWords)
        {
            foreach (var word in stopWords)
            {
                _stopWords.Add(word);
            }
        }

        public bool ProcessTerm(LazyStringValue source)
        {
            if (_stopWords.Contains(source))
                return false;
            return true;
        }
    }
}