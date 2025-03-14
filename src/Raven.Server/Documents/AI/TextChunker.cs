using System;
using System.Collections.Generic;
using System.Text;
using Corax.Pipeline;
using HtmlAgilityPack;
using Raven.Client.Documents.Operations.AI;

namespace Raven.Server.Documents.AI;

public static class TextChunker
{
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

    public static List<string> ChunkValue(string textualValue, ChunkingOptions chunkingOptions)
    {
        var list = new List<string>() { textualValue };
        return ChunkValues(list, chunkingOptions);
    }

    internal static List<string> SplitPlainText(string textualValue, int maxTokensPerChunk)
    {
        var tokenApproximationLen = textualValue.Length / 4;
        var whiteSpaceTokenizer = new WhitespaceTokenizer();
        var tokens = new List<Token>();
        
        whiteSpaceTokenizer.Tokenize(textualValue.AsSpan(), ref tokens);
        List<string> chunks = new(tokenApproximationLen / maxTokensPerChunk);
        
        var offset = 0;
        var currentChunkLenFromStart = 0;
        for (int i = 0; i < tokens.Count; i++)
        {
            var currentToken = tokens[i];
            currentChunkLenFromStart = currentToken.Offset + (int)currentToken.Length;

            if (i != 0 && (i+1) % maxTokensPerChunk == 0)
            {
                var subStr = textualValue.Substring(offset, currentChunkLenFromStart - offset);
                chunks.Add(subStr);
                offset = currentChunkLenFromStart;
                currentChunkLenFromStart = -1;
            }
        }

        if (currentChunkLenFromStart != -1)
        {
            var subStr = textualValue.Substring(offset, currentChunkLenFromStart - offset);
            chunks.Add(subStr);
        }

        return chunks;
    }
    
#pragma warning disable SKEXP0050
    public static List<string> ChunkValues(List<string> textualValues, ChunkingOptions chunkingOptions)
    {
        var chunkingMethod = chunkingOptions.ChunkingMethod;
        var maxTokensPerChunk = chunkingOptions.MaxTokensPerChunk;
        
        List<string> chunkedValues = new List<string>();
        List<string> chunkerResult;
        
        switch (chunkingMethod)
        {
            case ChunkingMethod.PlainTextSplit:
                foreach (var textualValue in textualValues)
                {
                    chunkerResult = SplitPlainText(textualValue, maxTokensPerChunk);
                    foreach (var chunkedValue in chunkerResult)
                        chunkedValues.Add(chunkedValue);
                }
                break;
            case ChunkingMethod.PlainTextSplitLines:
                foreach (var textualValue in textualValues)
                {
                    chunkerResult = Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextLines(textualValue, maxTokensPerChunk);
                    foreach (var chunkedValue in chunkerResult)
                        chunkedValues.Add(chunkedValue);
                }
                break;
            case ChunkingMethod.PlainTextSplitParagraphs:
                chunkerResult = Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextParagraphs(textualValues, maxTokensPerChunk);
                foreach (var chunkedValue in chunkerResult)
                    chunkedValues.Add(chunkedValue);
                break;
            case ChunkingMethod.MarkDownSplitLines:
                foreach (var textualValue in textualValues)
                {
                    chunkerResult = Microsoft.SemanticKernel.Text.TextChunker.SplitMarkDownLines(textualValue, maxTokensPerChunk);
                    foreach (var chunkedValue in chunkerResult)
                        chunkedValues.Add(chunkedValue);
                }
                break;
            case ChunkingMethod.MarkDownSplitParagraphs:
                chunkerResult = Microsoft.SemanticKernel.Text.TextChunker.SplitMarkdownParagraphs(textualValues, maxTokensPerChunk);
                foreach (var chunkedValue in chunkerResult)
                    chunkedValues.Add(chunkedValue);
                break;
            case ChunkingMethod.HtmlStrip:
                foreach (var textualValue in textualValues)
                {
                    var plainText = StripHtml(textualValue);
                    var chunks = SplitPlainText(plainText, maxTokensPerChunk);
                    chunkedValues.AddRange(chunks);
                }
                break;
            default:
                throw new ArgumentException($"Unrecognized chunking method - {chunkingMethod}");
        }

        return chunkedValues;
    }
#pragma warning restore SKEXP0050
}
