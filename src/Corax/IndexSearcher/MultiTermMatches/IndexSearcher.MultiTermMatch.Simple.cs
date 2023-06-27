using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Corax.Queries;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;

namespace Corax;

public partial class IndexSearcher
{
    /// <summary>
    /// Test API only
    /// </summary>
    public MultiTermMatch StartWithQuery(string field, string startWith, bool isNegated = false, bool hasBoost = false, bool forward = true) => StartWithQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), EncodeAndApplyAnalyzer(default, startWith), isNegated, forward);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(FieldMetadata field, string startWith, bool isNegated = false, bool forward = true)
    {
        if (forward == true)
        {
            return MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, isNegated);
        }
        else
        {
            return MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, isNegated);
        }
    }
    
    public MultiTermMatch StartWithQuery(FieldMetadata field, Slice startWith, bool isNegated = false, bool forward = true)
    {
        if (forward == true)
        {
            return MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, isNegated);
        }
        else
        {
            return MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, isNegated);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(FieldMetadata field, string endsWith, bool isNegated = false, bool forward = true)
    {
        if (forward == true)
        {
            return MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, isNegated);
        }
        else
        {
            return MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, isNegated);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(FieldMetadata field, Slice endsWith, bool isNegated = false, bool forward = true)
    {
        if (forward == true)
        {
            return MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, isNegated);
        }
        else
        {
            return MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, isNegated);
        }
    }
    
    public MultiTermMatch ContainsQuery(FieldMetadata field, string containsTerm, bool isNegated = false, bool forward = true) => ContainsQuery(field, EncodeAndApplyAnalyzer(field, containsTerm), isNegated, forward);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery(FieldMetadata field, Slice containsTerm, bool isNegated = false, bool forward = true)
    {
        if (forward == true)
        {
            return MultiTermMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, containsTerm, isNegated);
        }
        else
        {
            return MultiTermMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, containsTerm, isNegated);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery(FieldMetadata field, bool forward = true)
    {
        if (forward == true)
        {
            return MultiTermMatchBuilder<ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, default(Slice), false);
        }
        else
        {
            return MultiTermMatchBuilder<ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, default(Slice), false);
        }
    }

    public MultiTermMatch RegexQuery(FieldMetadata field, Regex regex, bool forward = true)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        if (forward == true)
        {
            return MultiTermMatch.Create(
                    new MultiTermMatch<RegexTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(
                        field, _transaction.Allocator, 
                        new RegexTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, terms, field, regex)
                    ));
        }
        else
        {
            return MultiTermMatch.Create(
                    new MultiTermMatch<RegexTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(
                        field, _transaction.Allocator,
                    new RegexTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, terms, field, regex)
                    ));
        }
    }
}
