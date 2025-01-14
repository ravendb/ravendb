#pragma warning disable SKEXP0070
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using FastBertTokenizer;
using Microsoft.ML.OnnxRuntime;
using Microsoft.SemanticKernel.Connectors.Onnx;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries.Vector;
using Raven.Server.Config;
using Sparrow;
using Sparrow.Server;
using InvalidOperationException = System.InvalidOperationException;
using VectorValue = Corax.Utils.VectorValue;

namespace Raven.Server.Documents.Indexes.VectorSearch;

public static class GenerateEmbeddings
{
    private static readonly ConstructorInfo BertOnnxTextEmbeddingGenerationServiceCtor;

    // Dimensions (buffer size) from internals of SmartComponents.
    private const int F32Size = 1536;

    private static SessionOptions OnnxSessionOptions;

    internal static readonly Lazy<BertOnnxTextEmbeddingGenerationService> Embedder = new(CreateTextEmbeddingGenerationService);

    static GenerateEmbeddings()
    {
        BertOnnxTextEmbeddingGenerationServiceCtor = typeof(BertOnnxTextEmbeddingGenerationService).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, [typeof(InferenceSession), typeof(BertTokenizer), typeof(int), typeof(BertOnnxOptions)]);
        if (BertOnnxTextEmbeddingGenerationServiceCtor == null)
            throw new InvalidOperationException($"Could not find constructor for {typeof(BertOnnxTextEmbeddingGenerationService)}.");
    }

    public static void Configure(RavenConfiguration configuration)
    {
        if (Embedder.IsValueCreated)
            throw new InvalidOperationException("Embedder has already been initialized.");
        
        OnnxSessionOptions = new SessionOptions() { IntraOpNumThreads = configuration.Indexing.MaxNumberOfThreadsForLocalEmbeddingsGeneration };
    }

    [ThreadStatic]
    private static List<string> List;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static VectorValue FromText(ByteStringContext allocator, in VectorOptions options, in string text)
    {
        var embedding = CreateEmbeddingViaSmartComponentsLocalEmbedding(allocator, text, F32Size);
        return options.DestinationEmbeddingType is VectorEmbeddingType.Single
            ? new VectorValue(embedding.MemoryScope, embedding.Memory, embedding.UsedBytes) :
            Quantize(allocator, options.DestinationEmbeddingType, embedding.MemoryScope, embedding.Memory, embedding.UsedBytes);
    }

    public static VectorValue FromArray(ByteStringContext allocator, IDisposable memoryScope, Memory<byte> memory, in VectorOptions options, int usedBytes)
    {
        var embeddingSourceType = options.SourceEmbeddingType;
        var embeddingDestinationType = options.DestinationEmbeddingType;
        switch (embeddingSourceType)
        {
            case VectorEmbeddingType.Binary or VectorEmbeddingType.Int8:
                PortableExceptions.ThrowIf<InvalidDataException>(embeddingDestinationType != embeddingSourceType);
                return new VectorValue(memoryScope, memory, usedBytes);
            case VectorEmbeddingType.Single when embeddingDestinationType is VectorEmbeddingType.Single:
                return new(memoryScope, memory, usedBytes);
            default:
                return Quantize(allocator, options.DestinationEmbeddingType, memoryScope, memory, usedBytes);
        }
    }

    public static VectorValue FromBase64Array(in VectorOptions options, ByteStringContext allocator, string base64, bool isAutoIndex = false)
    {
        var bytesRequired = (int)Math.Ceiling((base64.Length * 3) / 4.0); //this is approximation
        var memScope = allocator.Allocate(bytesRequired, out Memory<byte> mem);
        var result = Convert.TryFromBase64String(base64, mem.Span, out var bytesWritten);
        PortableExceptions.ThrowIf<InvalidDataException>(result == false, $"Excepted array encoded with base64, however got: '{base64}'. {(isAutoIndex == false ? string.Empty : $"{Environment.NewLine}If you want to create an embedding from a text, please wrap the field name in the method 'embedding.text(FieldName)'.")}");
        return FromArray(allocator, memScope, mem, options, bytesWritten);
    }

    private static VectorValue Quantize(ByteStringContext allocator, in VectorEmbeddingType destinationFormat,
        IDisposable memoryScope,
        Memory<byte> memory, int usedBytes)
    {
        if (destinationFormat is VectorEmbeddingType.Single)
            return new VectorValue(memoryScope, memory, usedBytes);

        VectorValue embeddings;
        var source = MemoryMarshal.Cast<byte, float>(memory.Span.Slice(0, usedBytes));

        switch (destinationFormat)
        {
            case VectorEmbeddingType.Int8:
                {
                    var dest = MemoryMarshal.Cast<byte, sbyte>(memory.Span);
                    if (dest.Length < source.Length + sizeof(float))
                    {
                        var requestedSize = dest.Length + sizeof(float);
                        var mem = allocator.Allocate(requestedSize, out System.Memory<byte> buffer);
                        VectorQuantizer.TryToInt8(source, MemoryMarshal.Cast<byte, sbyte>(buffer.Span), out usedBytes);

                        embeddings = new VectorValue(mem, buffer);
                        memoryScope.Dispose();
                    }
                    else
                    {
                        VectorQuantizer.TryToInt8(source, dest, out usedBytes);
                        embeddings = new VectorValue(memoryScope, memory, usedBytes);
                    }

                    embeddings.OverrideLength(usedBytes);
                    break;
                }
            case VectorEmbeddingType.Binary:
                {
                    var dest = MemoryMarshal.Cast<byte, byte>(memory.Span);
                    VectorQuantizer.TryToInt1(source, dest, out usedBytes);
                    embeddings = new VectorValue(memoryScope, memory, usedBytes);
                    break;
                }
            case VectorEmbeddingType.Single:
            case VectorEmbeddingType.Text:
            default:
                throw new ArgumentOutOfRangeException(nameof(destinationFormat), destinationFormat, null);
        }

        return embeddings;
    }

    private static (IDisposable MemoryScope, Memory<byte> Memory, int UsedBytes) CreateEmbeddingViaSmartComponentsLocalEmbedding(ByteStringContext allocator, in string text, in int dimensions)
    {
        List ??= new List<string>(1);
        List.Add(text);

        try
        {
            var embeddings = Embedder.Value.GenerateEmbeddingsAsync(List).GetAwaiter().GetResult();

            var embedding = embeddings[0];

            var memoryScope = allocator.Allocate(dimensions, out System.Memory<byte> memory);
            
            MemoryMarshal.AsBytes(embedding.Span).CopyTo(memory.Span);

            return (memoryScope, memory, dimensions);
        }
        finally
        {
            List.Clear();
        }
    }

    private static BertOnnxTextEmbeddingGenerationService CreateTextEmbeddingGenerationService()
    {
        using (var onnxModelStream = File.OpenRead(Path.Combine("LocalEmbeddings", "bge-micro-v2", "model.onnx")))
        using (var vocabStream = File.OpenRead(Path.Combine("LocalEmbeddings", "bge-micro-v2", "vocab.txt")))
        {
            if (onnxModelStream == null)
                throw new ArgumentNullException(nameof(onnxModelStream));
            if (vocabStream == null)
                throw new ArgumentNullException(nameof(vocabStream));

            var options = new BertOnnxOptions();

            var modelBytes = new MemoryStream();
            onnxModelStream.CopyTo(modelBytes);

            var onnxSession = new InferenceSession(modelBytes.Length == modelBytes.GetBuffer().Length ? modelBytes.GetBuffer() : modelBytes.ToArray(), OnnxSessionOptions ?? new SessionOptions());
            int dimensions = onnxSession.OutputMetadata.First().Value.Dimensions.Last();

            var tokenizer = new BertTokenizer();
            using (StreamReader vocabReader = new(vocabStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1024, leaveOpen: true))
            {
                tokenizer.LoadVocabulary(vocabReader, convertInputToLowercase: !options.CaseSensitive, options.UnknownToken, options.ClsToken, options.SepToken, options.PadToken, options.UnicodeNormalization);
            }

            return (BertOnnxTextEmbeddingGenerationService)BertOnnxTextEmbeddingGenerationServiceCtor.Invoke([onnxSession, tokenizer, dimensions, options]);
        }
    }
}
#pragma warning restore SKEXP0070
