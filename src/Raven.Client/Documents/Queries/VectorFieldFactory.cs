using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Numerics;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Extensions;
using Sparrow;
using Sparrow.Binary;

namespace Raven.Client.Documents.Queries;

public interface IVectorFieldFactory<T>
{
    public IVectorEmbeddingTextField WithText(string fieldName, VectorIndexingStrategy vectorIndexingStrategy = Constants.VectorSearch.DefaultIndexingStrategy);
    
    public IVectorEmbeddingTextField WithText(Expression<Func<T, object>> propertySelector, VectorIndexingStrategy vectorIndexingStrategy = Constants.VectorSearch.DefaultIndexingStrategy);
    
    public IVectorEmbeddingField WithEmbedding(string fieldName, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType, VectorIndexingStrategy vectorIndexingStrategy = Constants.VectorSearch.DefaultIndexingStrategy);
    
    public IVectorEmbeddingField WithEmbedding(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType, VectorIndexingStrategy vectorIndexingStrategy = Constants.VectorSearch.DefaultIndexingStrategy);
    
    public IVectorEmbeddingField WithBase64(string fieldName, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType, VectorIndexingStrategy vectorIndexingStrategy = Constants.VectorSearch.DefaultIndexingStrategy);
    
    public IVectorEmbeddingField WithBase64(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType, VectorIndexingStrategy vectorIndexingStrategy = Constants.VectorSearch.DefaultIndexingStrategy);

    public IVectorField WithField(string fieldName);
    
    public IVectorField WithField(Expression<Func<T, object>> propertySelector);
}

public interface IVectorEmbeddingTextField
{
    public IVectorEmbeddingTextField TargetQuantization(VectorEmbeddingType targetEmbeddingQuantization);
}

public interface IVectorEmbeddingField
{
    public IVectorEmbeddingField TargetQuantization(VectorEmbeddingType targetEmbeddingQuantization);
}

public interface IVectorField
{
    
}

internal sealed class VectorEmbeddingFieldFactory<T> : IVectorFieldFactory<T>, IVectorField, IVectorEmbeddingField, IVectorEmbeddingTextField
{
    private bool _byFieldMethodUsed;

    internal string FieldName { get; set; }
    internal VectorEmbeddingType SourceQuantizationType { get; set; } = Constants.VectorSearch.DefaultEmbeddingType;
    internal VectorEmbeddingType DestinationQuantizationType { get; set; } = Constants.VectorSearch.DefaultEmbeddingType;
    internal bool IsBase64Encoded { get; set; }
    internal VectorIndexingStrategy VectorIndexingStrategy { get; set; } = Constants.VectorSearch.DefaultIndexingStrategy;
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(Expression<Func<T, object>> propertySelector, VectorIndexingStrategy vectorIndexingStrategy)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = VectorEmbeddingType.Text;
        DestinationQuantizationType = Constants.VectorSearch.DefaultEmbeddingType;
        VectorIndexingStrategy = vectorIndexingStrategy;
        
        return this;
    }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(string fieldName, VectorIndexingStrategy vectorIndexingStrategy)
    {
        FieldName = fieldName;
        SourceQuantizationType = VectorEmbeddingType.Text;
        DestinationQuantizationType = Constants.VectorSearch.DefaultEmbeddingType;
        VectorIndexingStrategy = vectorIndexingStrategy;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithEmbedding(string fieldName, VectorEmbeddingType storedEmbeddingQuantization, VectorIndexingStrategy vectorIndexingStrategy)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        VectorIndexingStrategy = vectorIndexingStrategy;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithEmbedding(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization, VectorIndexingStrategy vectorIndexingStrategy)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        VectorIndexingStrategy = vectorIndexingStrategy;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithBase64(string fieldName, VectorEmbeddingType storedEmbeddingQuantization, VectorIndexingStrategy vectorIndexingStrategy)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        IsBase64Encoded = true;
        VectorIndexingStrategy = vectorIndexingStrategy;

        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithBase64(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization, VectorIndexingStrategy vectorIndexingStrategy)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        IsBase64Encoded = true;
        VectorIndexingStrategy = vectorIndexingStrategy;

        return this;
    }
    
    IVectorField IVectorFieldFactory<T>.WithField(string fieldName)
    {
        FieldName = fieldName;
        _byFieldMethodUsed = true;        
        return this;
    }

    IVectorField IVectorFieldFactory<T>.WithField(Expression<Func<T, object>> propertySelector)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        _byFieldMethodUsed = true;
        return this;
    }

    IVectorEmbeddingField IVectorEmbeddingField.TargetQuantization(VectorEmbeddingType targetEmbeddingQuantization)
    {
        PortableExceptions.ThrowIf<InvalidOperationException>(_byFieldMethodUsed, $"Cannot use method {nameof(IVectorEmbeddingField.TargetQuantization)} with {nameof(IVectorFieldFactory<T>.WithField)} since quantization is already done by the index.");
        
        DestinationQuantizationType = targetEmbeddingQuantization;
        
        if (SourceQuantizationType is VectorEmbeddingType.Int8 or VectorEmbeddingType.Binary && DestinationQuantizationType != SourceQuantizationType)
            throw new InvalidOperationException($"Cannot quantize already quantized embeddings. Source VectorEmbeddingType is {SourceQuantizationType}; however the destination is {DestinationQuantizationType}.");
        
        if (DestinationQuantizationType == VectorEmbeddingType.Text)
            throw new InvalidOperationException($"Cannot quantize the embedding to Text. This option is only available for {nameof(SourceQuantizationType)}.");
        
        return this;
    }

    IVectorEmbeddingTextField IVectorEmbeddingTextField.TargetQuantization(VectorEmbeddingType targetEmbeddingQuantization)
    {
        PortableExceptions.ThrowIf<InvalidOperationException>(_byFieldMethodUsed, $"Cannot use method {nameof(IVectorEmbeddingField.TargetQuantization)} with {nameof(IVectorFieldFactory<T>.WithField)} since quantization is already done by the index.");
        
        if (DestinationQuantizationType == VectorEmbeddingType.Text)
            throw new InvalidOperationException($"Cannot quantize the embedding to Text. This option is only available for {nameof(SourceQuantizationType)}.");
        
        DestinationQuantizationType = targetEmbeddingQuantization;

        return this;
    }
}

public interface IVectorEmbeddingTextFieldValueFactory
{
    public void ByText(string text);
}

public interface IVectorEmbeddingFieldValueFactory
{
    public void ByEmbedding<T>(IEnumerable<T> embedding) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    ;
    
    public void ByEmbedding<T>(T[] embedding) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    ;
    
    public void ByBase64(string base64Embedding);
}

public interface IVectorFieldValueFactory : IVectorEmbeddingTextFieldValueFactory, IVectorEmbeddingFieldValueFactory
{
    
}

internal class VectorFieldValueFactory : IVectorFieldValueFactory
{
    public object Embedding { get; set; }
    public string Text { get; set; }
    public string Base64Embedding { get; set; }
    
    void IVectorEmbeddingFieldValueFactory.ByEmbedding<T>(IEnumerable<T> embedding)
    {
        AssertEmbeddingType<T>();
        Embedding = embedding;
    }

    void IVectorEmbeddingFieldValueFactory.ByEmbedding<T>(T[] embedding)
    {
        AssertEmbeddingType<T>();
        Embedding = embedding;
    }

    private static void AssertEmbeddingType<T>()
    {
#if !NET7_0_OR_GREATER
        // For >=NET7, INumber<T> is the guardian.
        var isKnownType = typeof(T) == typeof(float) || typeof(T) == typeof(double) || typeof(T) == typeof(decimal) || typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(int) || typeof(T) == typeof(uint) || typeof(T) == typeof(long) || typeof(T) == typeof(ulong);
        
        if (isKnownType == false)
            throw new InvalidDataException($"The type of embedding must be numeric. Supported types are: float, double, decimal, sbyte, byte, int, uint, long, ulong. Received: {typeof(T).FullName}.");
#endif
    }
    
    void IVectorEmbeddingFieldValueFactory.ByBase64(string base64Embedding)
    {
        Base64Embedding = base64Embedding;
    }

    void IVectorEmbeddingTextFieldValueFactory.ByText(string text)
    {
        Text = text;
    }
}
