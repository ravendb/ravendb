using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using HtmlAgilityPack;
using Microsoft.ML.Tokenizers;
using Raven.Client.Documents.Operations.AI;

#pragma warning disable SKEXP0050

namespace Raven.Server.Documents.AI;

public static class TextChunker
{
    private static readonly ConcurrentDictionary<string, int> PrefixTokenCounts = new(StringComparer.Ordinal);
    
    public static List<string> Chunk(string textualValue, ChunkingOptions chunkingOptions)
    {
        var prefix = chunkingOptions.ContextPrefix;

        prefix = prefix?.TrimEnd();

        if (chunkingOptions.NoChunking)
            return ApplyPrefixWithoutChunking(textualValue, prefix);

        int effectiveMaxTokens = GetEffectiveMaxTokens(chunkingOptions.MaxTokensPerChunk, chunkingOptions.OverlapTokens, prefix);

        List<string> chunks = chunkingOptions.ChunkingMethod switch
        {
            ChunkingMethod.PlainTextSplit => ChunkPlainText(textualValue, effectiveMaxTokens),
            ChunkingMethod.PlainTextSplitLines => Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextLines(textualValue, effectiveMaxTokens),
            ChunkingMethod.HtmlStrip => ChunkPlainText(StripHtml(textualValue), effectiveMaxTokens),
            ChunkingMethod.MarkDownSplitLines => Microsoft.SemanticKernel.Text.TextChunker.SplitMarkDownLines(textualValue, effectiveMaxTokens),
            ChunkingMethod.PlainTextSplitParagraphs => Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextParagraphs([textualValue], effectiveMaxTokens, chunkingOptions.OverlapTokens),
            ChunkingMethod.MarkDownSplitParagraphs => Microsoft.SemanticKernel.Text.TextChunker.SplitMarkdownParagraphs([textualValue], effectiveMaxTokens, chunkingOptions.OverlapTokens),
            _ => throw new ArgumentOutOfRangeException(chunkingOptions.ChunkingMethod.ToString())
        };

        return ApplyPrefix(chunks, prefix);
    }

    private static int GetEffectiveMaxTokens(int maxTokensPerChunk, int overlapTokens, string prefix)
    {
        if (prefix is null)
            return maxTokensPerChunk;

        int prefixTokens = PrefixTokenCounts.GetOrAdd(prefix, static p => Tokenizer.CountTokens(p));
        int effectiveMaxTokensPerChunk = maxTokensPerChunk - prefixTokens;
        if (effectiveMaxTokensPerChunk <= 0)
            throw new InvalidOperationException(
                $"{nameof(ChunkingOptions.ContextPrefix)} is too long ({prefixTokens} tokens) for {nameof(ChunkingOptions.MaxTokensPerChunk)}={maxTokensPerChunk}. Increase {nameof(ChunkingOptions.MaxTokensPerChunk)} or shorten the {nameof(ChunkingOptions.ContextPrefix)}.");

        if (overlapTokens >= effectiveMaxTokensPerChunk)
            throw new InvalidOperationException(
                $"{nameof(ChunkingOptions.OverlapTokens)}={overlapTokens} is greater than or equal to the effective {nameof(ChunkingOptions.MaxTokensPerChunk)} ({effectiveMaxTokensPerChunk}) after subtracting {prefixTokens} tokens for {nameof(ChunkingOptions.ContextPrefix)} from {nameof(ChunkingOptions.MaxTokensPerChunk)}={maxTokensPerChunk}. Reduce {nameof(ChunkingOptions.OverlapTokens)} or shorten the {nameof(ChunkingOptions.ContextPrefix)}.");

        return effectiveMaxTokensPerChunk;
    }

    private static List<string> ApplyPrefix(List<string> chunks, string prefix)
    {
        if (prefix is null)
            return chunks;

        List<string> results = new(chunks.Count);
        foreach (var chunk in chunks)
        {
            if (string.IsNullOrWhiteSpace(chunk))
                continue;
            results.Add($"{prefix} {chunk}");
        }
        return results;
    }

    private static List<string> ApplyPrefixWithoutChunking(string textualValue, string prefix)
    {
        if (string.IsNullOrWhiteSpace(textualValue))
            return [];

        return prefix is null ? [textualValue] : [$"{prefix} {textualValue}"];
    }

    internal static string StripHtml(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        var htmlDoc = new HtmlDocument();
        htmlDoc.LoadHtml(input);

        var sb = new StringBuilder();
        ExtractPlainTextFromHtml(htmlDoc.DocumentNode, sb);

        var plainText = sb.ToString();

        return plainText;
    }

    private static void ExtractPlainTextFromHtml(HtmlNode node, StringBuilder sb)
    {
        foreach (var child in node.ChildNodes)
        {
            if (child.NodeType == HtmlNodeType.Text)
            {
                string text = child.InnerText.Trim();

                if (text.Length > 0)
                {
                    sb.Append(text);
                    sb.Append(" ");
                }
            }
            else if (child.NodeType == HtmlNodeType.Element)
            {
                ExtractPlainTextFromHtml(child, sb);
            }
        }
    }

    private static readonly Tokenizer Tokenizer = TiktokenTokenizer.CreateForEncoding("cl100k_base");

    public static List<string> ChunkPlainText(string textualValue, int maxTokensPerChunk)
    {
        var text = textualValue.AsMemory();
        var pos = Tokenizer.GetIndexByTokenCount(LimitTextSize(text, maxTokensPerChunk), maxTokensPerChunk, out var normalizedText, out var tokenCount,
            considerNormalization: false, considerPreTokenization: false);
        if (pos == text.Length) // avoid allocation if we can fit all tokens at once
        {
            return [textualValue];
        }

        List<string> results = [new(text[..pos].Span)];
        while(true)
        {
            text = text[pos..];
            if (text.IsEmpty)
                break;
            pos = Tokenizer.GetIndexByTokenCount(LimitTextSize(text, maxTokensPerChunk), maxTokensPerChunk, out normalizedText, out tokenCount,
                considerNormalization: false, considerPreTokenization: false);
            results.Add(new(text[..pos].Span));
        }

        return results;
    }

    private static ReadOnlySpan<char> LimitTextSize(ReadOnlyMemory<char> text, int maxTokens)
    {
        ReadOnlySpan<char> span = text.Span;
        if (span.Length <= maxTokens * 6)
            return span;
        // the issue is that CountTokens() use the whole string, but if we have a value that is ~500Kb in size,
        // and 2048 tokens, we'll spend huge amounts of time just splitting the whole string, since CountTokens()
        // has to work on the entire input we give it - therefor, we "guesstimate" the max size and pass that
        // to the CountTokens() function - most embeddings use 3 - 4 chars per token, so by using 6, we ensure that the
        // text we send will be longer than the max tokens
        return span[..(maxTokens * 6)];
    }
}
