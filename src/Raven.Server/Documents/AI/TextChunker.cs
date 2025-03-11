using System;
using System.Collections.Generic;
using Corax.Pipeline;
using HtmlAgilityPack;
using Raven.Client.Documents.Operations.AI;

namespace Raven.Server.Documents.AI;

public static class TextChunker
{
    private static string StripHtml(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        var htmlDoc = new HtmlDocument();
        htmlDoc.LoadHtml(input);
        return htmlDoc.DocumentNode.InnerText;
    }

    public static List<string> ChunkValue(string textualValue, ChunkingOptions chunkingOptions)
    {
        var list = new List<string>() { textualValue };
        return ChunkValues(list, chunkingOptions);
    }

    private static List<string> SplitPlainText(string textualValue, int maxTokensPerChunk)
    {
        var tokenApproximationLen = textualValue.Length / 4;
        var whiteSpaceTokenizer = new WhitespaceTokenizer();
        var tokens = new Token[tokenApproximationLen];
        var tokensAsRef = tokens.AsSpan();
        
        whiteSpaceTokenizer.Tokenize(textualValue.AsSpan(), ref tokensAsRef);
        List<string> chunks = new(tokenApproximationLen / maxTokensPerChunk);
        
        var offset = 0;
        var currentChunkLenFromStart = 0;
        for (int i = 0; i < tokensAsRef.Length; i++)
        {
            var currentToken = tokensAsRef[i];
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
                    chunkedValues.Add(StripHtml(textualValue));
                break;
            default:
                throw new ArgumentException($"Unrecognized chunking method - {chunkingMethod}");
        }

        return chunkedValues;
    }
#pragma warning restore SKEXP0050
}
