using System;
using Voron.Impl;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public class IndexedVectorsRetriever(LowLevelTransaction llt, string name) : IIndexedTermsRetriever
    {
        private readonly SearchState _searchState = new(llt, name);
        private long _lastReadNodeId = 1;

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            if (_lastReadNodeId > _searchState.Options.CountOfVectors)
            {
                term = [];
                return false;
            }

            _searchState.ReadNode(_lastReadNodeId, out var reader);
            term = reader.ReadVector(in _searchState).ToReadOnlySpan();
            _lastReadNodeId++;
            return true;
        }

        public ConvertTo Type => ConvertTo.Base64;
    }
}
