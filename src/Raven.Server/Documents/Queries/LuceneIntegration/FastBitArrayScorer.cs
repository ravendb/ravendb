using System.Buffers;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public class FastBitArrayScorer : Scorer
    {
        private FastBitArray _docs;
        private readonly bool _disposeArray;
        private IEnumerator<int> _enum;
        private int _currentDocId;
        internal FastBitArrayScorer(FastBitArray docs, Similarity similarity, bool disposeArray) : base(similarity)
        {
            _docs = docs;
            _disposeArray = disposeArray;
            _currentDocId = _docs.IndexOfFirstSetBit(); // may have a match on the first value
            _enum = _docs.Iterate(0).GetEnumerator();
        }

        public override int DocID()
        {
            return _currentDocId;
        }

        public override int NextDoc(IState state)
        {
            if (_enum?.MoveNext() == true)
            {
                _currentDocId = _enum.Current;
                return _currentDocId;
            }
            _enum?.Dispose();
            _enum = null;
            if (_disposeArray)
            {
                ArrayPool<ulong>.Shared.Return(_docs.Bits);
            }
            _currentDocId = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }

        public override int Advance(int target, IState state)
        {
            if (_docs.Disposed) 
                return NO_MORE_DOCS;
                
            _enum?.Dispose();
            _enum = _docs.Iterate(target).GetEnumerator();
            return NextDoc(state);
        }

        public override float Score(IState state)
        {
            return 1.0f;
        }
    }
}
