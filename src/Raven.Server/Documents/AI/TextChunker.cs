using System;
using System.Collections.Generic;
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
    
#pragma warning disable SKEXP0050
    public static List<string> ChunkValues(List<string> textualValues, ChunkingOptions chunkingOptions)
    {
        var chunkingMethod = chunkingOptions.ChunkingMethod;
        var maxTokensPerChunk = chunkingOptions.MaxTokensPerChunk;
        
        List<string> chunkedValues = new List<string>();
        List<string> chunkerResult;
        
        switch (chunkingMethod)
        {
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
            case ChunkingMethod.HtmlSplitLines:
                foreach (var textualValue in textualValues)
                {
                    chunkerResult = Microsoft.SemanticKernel.Text.TextChunker.SplitPlainTextLines(textualValue, maxTokensPerChunk);
                    foreach (var chunkedValue in chunkerResult)
                        chunkedValues.Add(StripHtml(chunkedValue));
                }
                break;
            default:
                throw new ArgumentException($"Unrecognized chunking method - {chunkingMethod}");
        }

        return chunkedValues;
    }
#pragma warning restore SKEXP0050
}
