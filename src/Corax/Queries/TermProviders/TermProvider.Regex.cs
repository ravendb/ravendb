using System.Collections.Generic;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries;

public struct RegexTermProvider : ITermProvider
{
    private readonly IndexSearcher _searcher;
    private readonly Regex _regex;
    private CompactTree.Iterator _iterator;
    private readonly CompactTree _tree;
    private readonly FieldMetadata _field;

    public RegexTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Regex regex)
    {
        _searcher = searcher;
        _regex = regex;
        _tree = tree;
        _iterator = tree.Iterate();
        _iterator.Reset();
        _field = field;
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

            term = _searcher.TermQuery(_field, _tree, termSlice);
            return true;
        }

        term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
        return false;
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(RegexTermProvider)}",
            parameters: new Dictionary<string, string>()
            {
                { "Field", _field.ToString() },
                { "Regex", _regex.ToString()}
            });
    }
}
