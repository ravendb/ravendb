using System;
using System.Linq;
using System.Runtime.CompilerServices;
namespace Raven.Client.Documents.Queries;

public class VectorQuantizer
{
    public static unsafe sbyte[] ToInt8(float[] rawEmbedding)
    {
        var maxComponent = rawEmbedding.Select(Math.Abs).Max();
        var scaleFactor = 127f / maxComponent;
        
        long sumOfSquaredMagnitudes = 0;
        
        sbyte[] embeddingRepresentation = new sbyte[rawEmbedding.Length + sizeof(float)];
        
        for (var i = 0; i < rawEmbedding.Length; i++)
        {
            var scaledValue = rawEmbedding[i] * scaleFactor;
            
            sumOfSquaredMagnitudes += (long)(scaledValue * scaledValue);
            
            embeddingRepresentation[i] = Convert.ToSByte(scaledValue);
        }
        
        *(float*)Unsafe.AsPointer(ref embeddingRepresentation[rawEmbedding.Length]) = (float)Math.Sqrt(sumOfSquaredMagnitudes);
        
        return embeddingRepresentation;
    }

    public static sbyte[] ToInt8(ReadOnlySpan<float> rawEmbedding) => ToInt8(rawEmbedding.ToArray());
    
    public static byte[] ToInt1(float[] embedding)
    {
        byte[] lookup = [0b_1000_0000, 0b_0100_0000, 0b_0010_0000, 0b_0001_0000, 0b_0000_1000, 0b_0000_0100, 0b_0000_0010, 0b_0000_0001];
        
        var inputLength = embedding.Length;
        var outputLength = inputLength % 8 == 0 ? inputLength / 8 : inputLength / 8 + 1;
        
        byte[] result = new byte[outputLength];
        
        for (var j = 0; j < inputLength; j++)
        {
            result[j / 8] |= embedding[j] >= 0 ? lookup[j % 8] : (byte)0;
        }
        
        return result;
    }
    
    public static byte[] ToInt1(ReadOnlySpan<float> rawEmbedding) => ToInt1(rawEmbedding.ToArray());
}
