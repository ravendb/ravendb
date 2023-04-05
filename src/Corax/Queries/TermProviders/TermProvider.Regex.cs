using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Text.Unicode;
using Corax.Mappings;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries;

public struct RegexTermProvider : ITermProvider
{
    private RegexTermProvider<CompactTree.ForwardIterator> _inner;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public RegexTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Regex regex)
    {
        _inner = new RegexTermProvider<CompactTree.ForwardIterator>(searcher, tree, field, regex);
    }

    public bool IsOrdered => _inner.IsOrdered;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset()
    {
        _inner.Reset();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Next(out TermMatch term)
    {
        return _inner.Next(out term);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public QueryInspectionNode Inspect()
    {
        return _inner.Inspect();
    }
}

public struct RegexTermProvider<TIterator> : ITermProvider
    where TIterator : struct, ICompactTreeIterator
{
    private readonly CompactTree _tree;
    private readonly IndexSearcher _searcher;
    private readonly FieldMetadata _field;
    private readonly Regex _regex;

    private TIterator _iterator;

    public bool IsOrdered => true;

    public RegexTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Regex regex)
    {
        _searcher = searcher;
        _regex = regex;
        _tree = tree;
        _iterator = tree.Iterate<TIterator>();
        _iterator.Reset();
        _field = field;
    }


    public void Reset()
    {
        _iterator = _tree.Iterate<TIterator>();
        _iterator.Reset();
    }

    public bool Next(out TermMatch term)
    {
        while (_iterator.MoveNext(out var termScope, out var _))
        {
            var key = termScope.Key.Decoded();
            if (_regex.IsMatch(Encoding.UTF8.GetString(key)) == false)
                continue;

            term = _searcher.TermQuery(_field, termScope.Key, _tree);
            return true;
        }

        term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
        return false;
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(RegexTermProvider<TIterator>)}",
            parameters: new Dictionary<string, string>()
            {
                { "Field", _field.ToString() },
                { "Regex", _regex.ToString()}
            });
    }
}
