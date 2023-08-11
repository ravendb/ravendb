using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Queries.TermProviders;

public struct RegexTermProvider<TLookupIterator> : ITermProvider
    where TLookupIterator : struct, ILookupIterator
{
    private readonly CompactTree _tree;
    private readonly IndexSearcher _searcher;
    private readonly FieldMetadata _field;
    private readonly Regex _regex;

    private CompactTree.Iterator<TLookupIterator> _iterator;

    public RegexTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Regex regex)
    {
        _searcher = searcher;
        _regex = regex;
        _tree = tree;
        _iterator = tree.Iterate<TLookupIterator>();
        _iterator.Reset();
        _field = field;
    }


    public bool IsFillSupported { get; }

    public int Fill(Span<long> containers)
    {
        throw new NotImplementedException();
    }

    public void Reset()
    {
        _iterator = _tree.Iterate<TLookupIterator>();
        _iterator.Reset();
    }

    public bool Next(out TermMatch term)
    {
        while (_iterator.MoveNext(out var compactKey, out var _))
        {
            var key = compactKey.Decoded();
            if (_regex.IsMatch(Encoding.UTF8.GetString(key)) == false)
                continue;

            term = _searcher.TermQuery(_field, compactKey, _tree);
            return true;
        }

        term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
        return false;
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(RegexTermProvider<TLookupIterator>)}",
            parameters: new Dictionary<string, string>()
            {
                { "Field", _field.ToString() },
                { "Regex", _regex.ToString()}
            });
    }
}
