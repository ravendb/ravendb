using SmartComponents.LocalEmbeddings;

namespace Raven.Server.Documents.Indexes.VectorSearch;

public class GenerateEmbeddings
{
    private static readonly LocalEmbedder Embedder = new();
    
    //TODO: Allocations!
    public static byte[] UsingI8(string str)
    {
        var e = Embedder.Embed<EmbeddingI1>(str);
        var arr = e.Buffer.ToArray();
        return arr;
    }

#pragma warning disable SKEXP0070 // ignore experimental warning 
    // private static readonly Lazy<BertOnnxTextEmbeddingGenerationService> EmbeddingGenerator = new(LoadOnnxModel);
    // private static BertOnnxTextEmbeddingGenerationService LoadOnnxModel()
    // {
    //     // TODO: Figure out distribution model
    //     // https://huggingface.co/SmartComponents/bge-micro-v2/resolve/72908b7/onnx/model_quantized.onnx
    //     // https://huggingface.co/SmartComponents/bge-micro-v2/resolve/72908b7/vocab.txt
    //     
    //     return BertOnnxTextEmbeddingGenerationService.Create(
    //         "C:\\Users\\ayende\\Downloads\\model_quantized.onnx",
    //         vocabPath: "C:\\Users\\ayende\\Downloads\\vocab.txt",
    //         new BertOnnxOptions { CaseSensitive = false, MaximumTokens = 512 });
    // }
    //
    // public static byte[] UsingI8(string str)
    // {
    //     var service = EmbeddingGenerator.Value;
    //     Task<IList<ReadOnlyMemory<float>>> generateEmbeddingsAsync = service.GenerateEmbeddingsAsync([str]);
    //     ReadOnlyMemory<float> readOnlyMemory = generateEmbeddingsAsync.Result.Single();
    //     var buffer = new byte[readOnlyMemory.Length / 8];
    //     QuantizeI8(readOnlyMemory.Span, buffer);
    //     return buffer;
    // }
    //
    //
    // private static void QuantizeI8(ReadOnlySpan<float> input, Span<byte> result)
    // {
    //     // https://github.com/dotnet/smartcomponents/blob/4dbb671443c84407b598a0104441afd1186d9a3a/src/SmartComponents.LocalEmbeddings/EmbeddingI1.cs
    //     var inputLength = input.Length;
    //     for (var j = 0; j < inputLength; j += 8)
    //     {
    //         // Vectorized approaches don't seem to get even close to the
    //         // speed of doing it in this naive way
    //         var sources = input.Slice(j, 8);
    //         var sum = (byte)0;
    //
    //         if (float.IsPositive(sources[0])) { sum |= 128; }
    //
    //         if (float.IsPositive(sources[1])) { sum |= 64; }
    //
    //         if (float.IsPositive(sources[2])) { sum |= 32; }
    //
    //         if (float.IsPositive(sources[3])) { sum |= 16; }
    //
    //         if (float.IsPositive(sources[4])) { sum |= 8; }
    //
    //         if (float.IsPositive(sources[5])) { sum |= 4; }
    //
    //         if (float.IsPositive(sources[6])) { sum |= 2; }
    //
    //         if (float.IsPositive(sources[7])) { sum |= 1; }
    //
    //         result[j / 8] = sum;
    //     }
    // }

#pragma warning restore SKEXP0070
}
