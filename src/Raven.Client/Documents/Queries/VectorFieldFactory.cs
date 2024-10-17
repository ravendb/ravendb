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
    public IVectorEmbeddingTextField WithText(string fieldName);
    
    public IVectorEmbeddingTextField WithText(Expression<Func<T, object>> propertySelector);
    
    public IVectorEmbeddingField WithEmbedding(string fieldName, EmbeddingType storedEmbeddingQuantization = EmbeddingType.None);
    
    public IVectorEmbeddingField WithEmbedding(Expression<Func<T, object>> propertySelector, EmbeddingType storedEmbeddingQuantization = EmbeddingType.None);
    public IVectorEmbeddingField WithBase64(string fieldName, EmbeddingType storedEmbeddingQuantization = EmbeddingType.None);
    
    public IVectorEmbeddingField WithBase64(Expression<Func<T, object>> propertySelector, EmbeddingType storedEmbeddingQuantization = EmbeddingType.None);

    public IVectorField WithField(string fieldName, EmbeddingType storedEmbeddingQuantization = EmbeddingType.None);
    
    public IVectorField WithField(Expression<Func<T, object>> propertySelector, EmbeddingType storedEmbeddingQuantization = EmbeddingType.None);
}

public interface IVectorEmbeddingTextField
{
    
}

public interface IVectorEmbeddingField
{
    public IVectorEmbeddingField TargetQuantization(EmbeddingType targetEmbeddingQuantization);
}

public interface IVectorField : IVectorEmbeddingField, IVectorEmbeddingTextField
{
    
}

internal sealed class VectorEmbeddingFieldFactory<T> : IVectorFieldFactory<T>, IVectorField
{
    internal string FieldName { get; set; }
    internal EmbeddingType SourceQuantizationType { get; set; }
    internal EmbeddingType DestinationQuantizationType { get; set; }
    internal bool IsBase64Encoded { get; set; }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(Expression<Func<T, object>> propertySelector)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);

        return this;
    }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(string fieldName)
    {
        FieldName = fieldName;

        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithEmbedding(string fieldName, EmbeddingType storedEmbeddingQuantization)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithEmbedding(Expression<Func<T, object>> propertySelector, EmbeddingType storedEmbeddingQuantization)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithBase64(string fieldName, EmbeddingType storedEmbeddingQuantization)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        IsBase64Encoded = true;

        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithBase64(Expression<Func<T, object>> propertySelector, EmbeddingType storedEmbeddingQuantization)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        IsBase64Encoded = true;

        return this;
    }

    IVectorField IVectorFieldFactory<T>.WithField(string fieldName, EmbeddingType storedEmbeddingQuantization)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        
        return this;
    }

    IVectorField IVectorFieldFactory<T>.WithField(Expression<Func<T, object>> propertySelector, EmbeddingType storedEmbeddingQuantization)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        
        return this;
    }

    IVectorEmbeddingField IVectorEmbeddingField.TargetQuantization(EmbeddingType targetEmbeddingQuantization)
    {
        DestinationQuantizationType = targetEmbeddingQuantization;
        
        if (DestinationQuantizationType < SourceQuantizationType)
            throw new Exception($"Cannot quantize vector with {SourceQuantizationType} quantization into {DestinationQuantizationType}");

        if (SourceQuantizationType == EmbeddingType.Int8 && DestinationQuantizationType == EmbeddingType.Binary)
            throw new Exception("Cannot quantize already quantized embeddings");
        
        return this;
    }
}

public interface IVectorEmbeddingTextFieldValueFactory
{
    public void ByText(string text);
}

public interface IVectorEmbeddingFieldValueFactory
{
    public void ByEmbedding<T>(IEnumerable<T> embedding, EmbeddingType queriedEmbeddingQuantization = EmbeddingType.None) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    ;
    
    public void ByBase64(string base64Embedding, EmbeddingType queriedEmbeddingQuantization = EmbeddingType.None);
}

public interface IVectorFieldValueFactory : IVectorEmbeddingTextFieldValueFactory, IVectorEmbeddingFieldValueFactory
{
    
}

internal class VectorFieldValueFactory : IVectorFieldValueFactory
{
    public object Embedding { get; set; }
    public string Text { get; set; }
    public string Base64Embedding { get; set; }
    public EmbeddingType EmbeddingType { get; set; }
    
    void IVectorEmbeddingFieldValueFactory.ByEmbedding<T>(IEnumerable<T> embedding, EmbeddingType queriedEmbeddingQuantization)
    {
#if NET7_0_OR_GREATER == FALSE
        // For >=NET7, INumber<T> is the guardian.
        var isKnownType = typeof(T) == typeof(float) || typeof(T) == typeof(double) || typeof(T) == typeof(decimal) || typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(int) || typeof(T) == typeof(uint) || typeof(T) == typeof(long) || typeof(T) == typeof(ulong);
        
        if (isKnownType == false)
            throw new InvalidDataException($"The type of embedding must be numeric. Supported types are: float, double, decimal, sbyte, byte, int, uint, long, ulong. Received: {typeof(T).FullName}.");
#endif
        
        Embedding = embedding;
        EmbeddingType = queriedEmbeddingQuantization;
    }

    void IVectorEmbeddingFieldValueFactory.ByBase64(string base64Embedding, EmbeddingType queriedEmbeddingQuantization)
    {
        Base64Embedding = base64Embedding;
        EmbeddingType = queriedEmbeddingQuantization;
    }

    void IVectorEmbeddingTextFieldValueFactory.ByText(string text)
    {
        Text = text;
    }
}
