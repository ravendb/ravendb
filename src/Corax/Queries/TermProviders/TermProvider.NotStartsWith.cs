using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Corax.Mappings;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Queries.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct NotStartsWithTermProvider<TLookupIterator> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _startWith;
        private readonly bool _validatePostfixLen;
        private readonly CancellationToken _token;

        private CompactTree.Iterator<TLookupIterator> _iterator;


        public NotStartsWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey startWith, bool validatePostfixLen, CancellationToken token)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TLookupIterator>();
            _iterator.Reset();
            _startWith = startWith;
            _validatePostfixLen = validatePostfixLen;
            _token = token;
            _tree = tree;
        }

        public bool IsFillSupported { get; }

        public int Fill(Span<long> containers)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            var startWith = _startWith.Decoded();
            while (_iterator.MoveNext(out var key, out _, out _))
            {
                _token.ThrowIfCancellationRequested();
                var termSlice = key.Decoded();
                
                if (termSlice.StartsWith(startWith))
                {
                    if (_validatePostfixLen == false || // starts with the prefix, so skip
                        termSlice[^1] == startWith.Length) // didn't pass the postfix marker validation,see: RavenDB-21131
                    {
                        continue;
                    }
                }

                term = _searcher.TermQuery(_field, key, _tree);
                return true;
            }
            
            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotStartsWithTermProvider<TLookupIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        string DebugView => Inspect().ToString();
    }
}
