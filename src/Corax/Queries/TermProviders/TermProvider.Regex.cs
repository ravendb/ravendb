using System.Collections.Generic;
using System.Text.RegularExpressions;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries;

public struct RegexTermProvider : ITermProvider
{
    private readonly IndexSearcher _searcher;
    private readonly Regex _regex;
    private CompactTree.Iterator _iterator;
    private readonly CompactTree _tree;
    private readonly string _fieldName;

    public RegexTermProvider(IndexSearcher searcher, CompactTree tree, string field, Regex regex)
    {
        _searcher = searcher;
        _regex = regex;
        _tree = tree;
        _iterator = tree.Iterate();
        _iterator.Reset();
        _fieldName = field;
    }


    public void Reset()
    {
        _iterator = _tree.Iterate();
        _iterator.Reset();
    }

    public bool Next(out TermMatch term)
    {
        while (_iterator.MoveNext(out Slice termSlice, out var _))
        {
            if (_regex.IsMatch(termSlice.ToString()) == false)
                continue;

            term = _searcher.TermQuery(_tree, termSlice);
            return true;
        }

        term = TermMatch.CreateEmpty(_searcher.Allocator);
        return false;
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(RegexTermProvider)}",
            parameters: new Dictionary<string, string>()
            {
                { "Field", _fieldName },
                { "Regex", _regex.ToString()}
            });
    }
}
