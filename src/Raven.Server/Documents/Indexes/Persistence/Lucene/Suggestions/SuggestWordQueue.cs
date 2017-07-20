using Lucene.Net.Util;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions
{
    internal sealed class SuggestWordQueue : PriorityQueue<SuggestWord>
    {
        internal SuggestWordQueue(int size)
        {
            Initialize(size);
        }

        public override bool LessThan(SuggestWord a, SuggestWord b)
        {
            var val = a.CompareTo(b);
            return val < 0;
        }
    }
}