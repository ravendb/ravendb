using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermProviders;

public struct RegexTermProvider<TLookupIterator> : ITermProvider
    where TLookupIterator : struct, ILookupIterator
{
    private readonly CompactTree _tree;
    private readonly Querying.IndexSearcher _searcher;
    private readonly FieldMetadata _field;
    private readonly Regex _regex;

    private CompactTree.Iterator<TLookupIterator> _iterator;

    public RegexTermProvider(Querying.IndexSearcher searcher, CompactTree tree, in FieldMetadata field, Regex regex)
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
        char[] buffer = null;
        try
        {
            while (_iterator.MoveNext(out var compactKey, out _, out _))
            {
                var key = compactKey.Decoded();
                if (_regex.IsMatch(ToChars(key, ref buffer)) == false)
                    continue;

                term = _searcher.TermQuery(_field, compactKey, _tree);
                return true;
            }
        }
        finally
        {
            if (buffer != null)
                ArrayPool<char>.Shared.Return(buffer);
        }

        term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
        return false;
    }

    // Decode the term's UTF-8 bytes into a pooled char buffer (no per-term string allocation) and return the
    // written slice. Sized with GetMaxCharCount (an O(1) upper bound) rather than GetCharCount, which would
    // re-scan every term's bytes just to size the buffer.
    private static ReadOnlySpan<char> ToChars(ReadOnlySpan<byte> key, ref char[] buffer)
    {
        int maxChars = Encoding.UTF8.GetMaxCharCount(key.Length);
        if (buffer == null || buffer.Length < maxChars)
        {
            if (buffer != null)
                ArrayPool<char>.Shared.Return(buffer);
            buffer = ArrayPool<char>.Shared.Rent(maxChars);
        }

        int written = Encoding.UTF8.GetChars(key, buffer);
        return buffer.AsSpan(0, written);
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(RegexTermProvider<TLookupIterator>)}",
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _field.ToString() },
                { Constants.QueryInspectionNode.Term, _regex.ToString()}
            });
    }
}
