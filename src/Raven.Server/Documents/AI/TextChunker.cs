using System;
using System.Collections.Generic;
using System.Text;
using HtmlAgilityPack;
using Microsoft.ML.Tokenizers;
using Raven.Client.Documents.Operations.AI;

#pragma warning disable SKEXP0050

namespace Raven.Server.Documents.AI;

public static class TextChunker
{
    public static List<string> Chunk(string textualValue, ChunkingOptions chunkingOptions)
    {
        switch (chunkingOptions.ChunkingMethod)
        {
            case ChunkingMethod.PlainTextSplit:
                return ChunkPlainText(textualValue, chunkingOptions.MaxTokensPerChunk);
            case ChunkingMethod.PlainTextSplitLines:
                return Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextLines(textualValue, chunkingOptions.MaxTokensPerChunk);
            case ChunkingMethod.HtmlStrip:
                return ChunkPlainText(StripHtml(textualValue), chunkingOptions.MaxTokensPerChunk);
            case ChunkingMethod.MarkDownSplitLines:
                return Microsoft.SemanticKernel.Text.TextChunker.SplitMarkDownLines(textualValue, chunkingOptions.MaxTokensPerChunk);

            case ChunkingMethod.PlainTextSplitParagraphs:
                return Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextParagraphs([textualValue], chunkingOptions.MaxTokensPerChunk);
            case ChunkingMethod.MarkDownSplitParagraphs:
                return Microsoft.SemanticKernel.Text.TextChunker.SplitMarkdownParagraphs([textualValue], chunkingOptions.MaxTokensPerChunk);
            default:
                throw new ArgumentOutOfRangeException(chunkingOptions.ChunkingMethod.ToString());
        }
    }
    
    public static List<string> Chunk(List<string> textualValues, ChunkingOptions chunkingOptions)
    {
        switch (chunkingOptions.ChunkingMethod)
        {
            case ChunkingMethod.PlainTextSplitParagraphs:
                return Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextParagraphs(textualValues, chunkingOptions.MaxTokensPerChunk);
            case ChunkingMethod.MarkDownSplitParagraphs:
                return Microsoft.SemanticKernel.Text.TextChunker.SplitMarkdownParagraphs(textualValues, chunkingOptions.MaxTokensPerChunk);
        }

        List<string> results = [];
        foreach (string textualValue in textualValues)
        {
            switch (chunkingOptions.ChunkingMethod)
            {
                case ChunkingMethod.PlainTextSplit:
                    results.AddRange(ChunkPlainText(textualValue, chunkingOptions.MaxTokensPerChunk));
                    break;
                case ChunkingMethod.PlainTextSplitLines:
                    results.AddRange(Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextLines(textualValue, chunkingOptions.MaxTokensPerChunk));
                    break;
                case ChunkingMethod.HtmlStrip:
                    results.AddRange( ChunkPlainText(StripHtml(textualValue), chunkingOptions.MaxTokensPerChunk));
                    break;
                case ChunkingMethod.MarkDownSplitLines:
                    results.AddRange( Microsoft.SemanticKernel.Text.TextChunker.SplitMarkDownLines(textualValue, chunkingOptions.MaxTokensPerChunk));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(chunkingOptions.ChunkingMethod.ToString());
            }
        }
        return results;
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
        var pos = Tokenizer.GetIndexByTokenCount(text.Span, maxTokensPerChunk, out var normalizedText, out var tokenCount,
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
            pos = Tokenizer.GetIndexByTokenCount(text.Span, maxTokensPerChunk, out normalizedText, out tokenCount,
                considerNormalization: false, considerPreTokenization: false);
            results.Add(new(text[..pos].Span));
        } 

        return results;
    }
    
}
