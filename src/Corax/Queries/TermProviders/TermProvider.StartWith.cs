using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Queries.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct StartsWithTermProvider<TLookupIterator> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _startWith;
        private readonly CompactKey _startWithLimit;
        private readonly bool _validatePostfixLen;
        private readonly CancellationToken _token;
        private bool _firstRun;

        private CompactTree.Iterator<TLookupIterator> _iterator;

        public StartsWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey startWith, CompactKey seekTerm, bool validatePostfixLen, CancellationToken token)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TLookupIterator>();
            _startWith = startWith;
            _startWithLimit = seekTerm;
            _validatePostfixLen = validatePostfixLen;
            _token = token;
            _tree = tree;

            Reset();
        }

        public bool IsFillSupported { get; }

        public int Fill(Span<long> containers)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            if (default(TLookupIterator).IsForward)
            {
                _iterator.Seek(_startWith);
                return;
            }
            
            _firstRun = true;
            _iterator.Seek(_startWithLimit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Next(out TermMatch term)
        {
            ReadOnlySpan<byte> decodedStartsWith = _startWith.Decoded();

            CompactKey compactKey;
            ReadOnlySpan<byte> key;
            while (true)
            {
                if (_iterator.MoveNext(out compactKey, out _, out _) == false)
                {
                    term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                    return false;
                }

                key = compactKey.Decoded();
                if (_validatePostfixLen == false  || 
                    key[^1] == decodedStartsWith.Length)
                {
                     break;
                }
                // otherwise, it isn't a match, see: RavenDB-21131, it just a shared prefix
                // that we need to skip
                _token.ThrowIfCancellationRequested();
            }
            
            //For backward iterator we can have two possibilities:
            //a) we're already on valid last term (when startsWith starts with [255]...[255][...] or our prefix is the last in tree)
            //b) we're on next term from our initial startsWith (e.g. for prefix 'ab' we have to seek a['b'+1])
            if (_firstRun && default(TLookupIterator).IsForward == false && key.StartsWith(decodedStartsWith) == false)
            {
                _firstRun = false;
                return Next(out term);
            }

            if (key.StartsWith(decodedStartsWith) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            term = _searcher.TermQuery(_field, compactKey, _tree);
            return true;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(StartsWithTermProvider<TLookupIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
