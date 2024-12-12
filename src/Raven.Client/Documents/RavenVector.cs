using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;

namespace Raven.Client.Documents;

public class RavenVector<T> : IEnumerable
    where T : unmanaged
#if NET7_0_OR_GREATER
    , INumber<T>
#endif
{
    public T[] Embedding { get; set; }
    
    public RavenVector(IEnumerable<T> embedding)
    {
        AssertEmbeddingType();
        Embedding = embedding.ToArray();
    }

    public RavenVector(T[] embedding)
    {
        AssertEmbeddingType();
        Embedding = embedding;
    }

    public static implicit operator RavenVector<T>(T[] array)
    {
        return new RavenVector<T>(array);
    }
    
    internal Type GetEmbeddingType() => typeof(T);
    
    private static void AssertEmbeddingType()
    {
#if !NET7_0_OR_GREATER
        // For >=NET7, INumber<T> is the guardian.
        var isKnownType = typeof(T) == typeof(float) || typeof(T) == typeof(double) || typeof(T) == typeof(decimal) || typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(int) || typeof(T) == typeof(uint) || typeof(T) == typeof(long) || typeof(T) == typeof(ulong);
        
        if (isKnownType == false)
            throw new InvalidDataException($"The type of embedding must be numeric. Supported types are: float, double, decimal, sbyte, byte, int, uint, long, ulong. Received: {typeof(T).FullName}.");
#endif
    }

    public IEnumerator GetEnumerator()
    {
        return Embedding.GetEnumerator();
    }
}
