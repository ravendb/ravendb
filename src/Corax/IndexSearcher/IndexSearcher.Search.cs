using System;
using System.Data;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Pipeline;
using Corax.Queries;
using Sparrow.Server;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
    public IQueryMatch SearchQuery(string field, string searchTerm, Constants.Search.Operator @operator, bool isNegated, int analyzerId)
    {
        return SearchQuery<NullScoreFunction>(field, searchTerm, default, @operator, analyzerId, isNegated);
    }

    public IQueryMatch SearchQuery<TScoreFunction>(string field, string searchTerm, TScoreFunction scoreFunction, Constants.Search.Operator @operator, int analyzerId,
        bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
    {
        ReadOnlySpan<byte> term = Encoding.UTF8.GetBytes(searchTerm).AsSpan();
        if (isNegated)
        {
            @operator = @operator switch
            {
                Constants.Search.Operator.Or => Constants.Search.Operator.And,
                Constants.Search.Operator.And => Constants.Search.Operator.Or,
                _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
            };
        }

        if (_fieldMapping.TryGetByFieldId(analyzerId, out var indexFieldBinding) == false && indexFieldBinding.Analyzer is null)
        {
            throw new InvalidOperationException($"{nameof(SearchQuery)} requires analyzer.");
        }

        var searchAnalyzer = indexFieldBinding.Analyzer;
        var wildcardAnalyzer = Analyzer.Create<WhitespaceTokenizer, ExactTransformer>();

        searchAnalyzer.GetOutputBuffersSize(term.Length, out var outputSize, out var tokenSize);
        wildcardAnalyzer.GetOutputBuffersSize(term.Length, out var wildcardSize, out var wildcardTokenSize);

        var tokenStructSize = Unsafe.SizeOf<Token>();
        var buffer = QueryContext.MatchesRawPool.Rent(outputSize + wildcardSize + tokenStructSize * (tokenSize + wildcardTokenSize));
        Span<byte> encodeBufferOriginal = buffer.AsSpan().Slice(0, outputSize);
        Span<Token> tokensBufferOriginal = MemoryMarshal.Cast<byte, Token>(buffer.AsSpan().Slice(outputSize, tokenSize * tokenStructSize));

        var wildcardAnalyzerBuffer = buffer.AsSpan().Slice(outputSize + tokenSize * tokenStructSize, wildcardSize);
        var wildcardTokenizerBuffer = MemoryMarshal.Cast<byte, Token>(buffer.AsSpan().Slice(outputSize + tokenSize * tokenStructSize + wildcardSize));

        var encodedWildcard = wildcardAnalyzerBuffer;
        var tokensWildcard = wildcardTokenizerBuffer;


        wildcardAnalyzer.Execute(term, ref encodedWildcard, ref tokensWildcard);
        IQueryMatch match = null;
        foreach (var tokenWildcard in tokensWildcard)
        {
            var encoded = encodeBufferOriginal;
            var tokens = tokensBufferOriginal;
            var originalWord = term.Slice(tokenWildcard.Offset, (int)tokenWildcard.Length);
            searchAnalyzer.Execute(term.Slice(tokenWildcard.Offset, (int)tokenWildcard.Length), ref encoded, ref tokens);
            if (tokens.Length == 0)
                continue;


            Constants.Search.SearchMatchOptions mode = Constants.Search.SearchMatchOptions.TermMatch;
            if (originalWord[0] is Constants.Search.Wildcard)
                mode |= Constants.Search.SearchMatchOptions.EndsWith;
            if (originalWord[^1] is Constants.Search.Wildcard)
                mode |= Constants.Search.SearchMatchOptions.StartsWith;
            if (mode.HasFlag(Constants.Search.SearchMatchOptions.StartsWith | Constants.Search.SearchMatchOptions.EndsWith))
                mode = Constants.Search.SearchMatchOptions.Contains;

            Slice.From(_transaction.Allocator, encoded.Slice(tokens[0].Offset, (int)tokens[0].Length), ByteStringType.Immutable, out var encodedString);
            BuildExpression(mode, encodedString);
        }

        QueryContext.MatchesRawPool.Return(buffer);

        return typeof(TScoreFunction) == typeof(NullScoreFunction)
            ? match
            : Boost(match, scoreFunction);

        void BuildExpression(Constants.Search.SearchMatchOptions mode, Slice encodedString)
        {
            switch (mode)
            {
                case Constants.Search.SearchMatchOptions.TermMatch:
                    IQueryMatch exactMatch = isNegated
                        ? UnaryQuery(AllEntries(), analyzerId, encodedString, UnaryMatchOperation.NotEquals)
                        : TermQuery(field, encodedString.ToString());

                    if (match is null)
                    {
                        match = exactMatch;
                        return;
                    }

                    match = @operator is Constants.Search.Operator.Or
                        ? Or(match, exactMatch)
                        : And(match, exactMatch);
                    break;
                case Constants.Search.SearchMatchOptions.StartsWith:
                    if (match is null)
                    {
                        match = StartWithQuery(field, encodedString.ToString(), isNegated);
                        return;
                    }

                    match = @operator is Constants.Search.Operator.Or
                        ? Or(match, StartWithQuery(field, encodedString.ToString(), isNegated))
                        : And(match, StartWithQuery(field, encodedString.ToString(), isNegated));
                    break;
                case Constants.Search.SearchMatchOptions.EndsWith:
                    if (match is null)
                    {
                        match = EndsWithQuery(field, encodedString.ToString(), isNegated);
                        return;
                    }

                    match = @operator is Constants.Search.Operator.Or
                        ? Or(match, EndsWithQuery(field, encodedString.ToString(), isNegated))
                        : And(match, EndsWithQuery(field, encodedString.ToString(), isNegated));
                    break;
                case Constants.Search.SearchMatchOptions.Contains:
                    if (match is null)
                    {
                        match = ContainsQuery(field, encodedString.ToString(), isNegated);
                        return;
                    }

                    match = @operator is Constants.Search.Operator.Or
                        ? Or(match, ContainsQuery(field, encodedString.ToString(), isNegated))
                        : And(match, ContainsQuery(field, encodedString.ToString(), isNegated));
                    break;
                default:
                    throw new InvalidExpressionException("Unknown flag inside Search match.");
            }
        }
    }
}
