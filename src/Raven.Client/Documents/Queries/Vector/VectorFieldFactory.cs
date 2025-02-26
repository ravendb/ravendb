using System;
using System.Collections.Generic;
using System.IO; // do not delete
using System.Linq.Expressions;
using System.Numerics; // do not delete
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Extensions;
using Sparrow;

namespace Raven.Client.Documents.Queries.Vector;

public interface IVectorFieldFactory<T>
{
    /// <summary>
    /// Defines the text field that vector search will be performed on.
    /// </summary>
    /// <param name="fieldName">Name of the document field containing text data.</param>
    /// <param name="aiTaskName">Name of the AI task that will be used to generate embedding for queried text.</param>
    public IVectorEmbeddingTextField WithText(string fieldName, string aiTaskName = null);
    
    /// <inheritdoc cref="WithText(string,Raven.Client.Documents.Indexes.Vector.VectorIndexingStrategy)"/>
    /// <param name="propertySelector">Path to the document field containing text data.</param>
    public IVectorEmbeddingTextField WithText(Expression<Func<T, object>> propertySelector, string aiTaskName = null);
    
    /// <summary>
    /// Defines the embedding field that vector search will be performed on.
    /// </summary>
    /// <param name="fieldName">Name of the document field containing embedding data.</param>
    /// <param name="storedEmbeddingQuantization">Quantization that was performed on stored embeddings.</param>
    public IVectorEmbeddingField WithEmbedding(string fieldName, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType);
    
    ///<inheritdoc cref="WithEmbedding(string,Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType,Raven.Client.Documents.Indexes.Vector.VectorIndexingStrategy)"/>
    /// <param name="propertySelector">Path to the document field containing embedding data.</param>
    public IVectorEmbeddingField WithEmbedding(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType);
    
    /// <summary>
    /// Defines the embedding field (encoded as base64) that vector search will be performed on.
    /// </summary>
    /// <param name="fieldName">Name of the document field containing base64 encoded embedding data.</param>
    /// <param name="storedEmbeddingQuantization">Quantization of stored embeddings.</param>
    public IVectorEmbeddingField WithBase64(string fieldName, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType);
    
    /// <inheritdoc cref="WithBase64(string,Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType,Raven.Client.Documents.Indexes.Vector.VectorIndexingStrategy)"/>
    public IVectorEmbeddingField WithBase64(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType);

    /// <summary>
    /// Defines the field (that's already indexed) that vector search will be performed on.
    /// </summary>
    /// <param name="fieldName">Name of the index-field containing indexed data.</param>
    public IVectorField WithField(string fieldName);
    
    /// <inheritdoc cref="WithField(string)"/>
    /// <param name="propertySelector">Path to the index-field containing indexed data.</param>
    public IVectorField WithField(Expression<Func<T, object>> propertySelector);
}

public interface IVectorEmbeddingTextField
{
    /// <summary>
    /// Defines quantization that will be performed on embeddings that are already in the database.
    /// </summary>
    /// <param name="targetEmbeddingQuantization">Desired target quantization type.</param>
    public IVectorEmbeddingTextField TargetQuantization(VectorEmbeddingType targetEmbeddingQuantization);
}

public interface IVectorEmbeddingField
{
    /// <inheritdoc cref="IVectorEmbeddingTextField.TargetQuantization(Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType)"/>
    public IVectorEmbeddingField TargetQuantization(VectorEmbeddingType targetEmbeddingQuantization);
}

public interface IVectorField
{
    
}

public interface IVectorEmbeddingFieldFactoryAccessor
{
    internal string FieldName { get; set; }
    internal VectorEmbeddingType SourceQuantizationType { get; set; }
    internal VectorEmbeddingType DestinationQuantizationType { get; set; } 
    internal bool IsBase64Encoded { get; set; }
    internal string EtlConfigName { get; set; }
}

internal sealed class VectorEmbeddingFieldFactory<T> : IVectorFieldFactory<T>, IVectorField, IVectorEmbeddingField, IVectorEmbeddingTextField, IVectorEmbeddingFieldFactoryAccessor
{
    private bool _byFieldMethodUsed;
    public string FieldName { get; set; }
    public VectorEmbeddingType SourceQuantizationType { get; set; } = Constants.VectorSearch.DefaultEmbeddingType;
    public VectorEmbeddingType DestinationQuantizationType { get; set; } = Constants.VectorSearch.DefaultEmbeddingType;
    public bool IsBase64Encoded { get; set; }
    public string EtlConfigName { get; set; }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(Expression<Func<T, object>> propertySelector, string etlConfigName)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = VectorEmbeddingType.Text;
        DestinationQuantizationType = Constants.VectorSearch.DefaultEmbeddingType;
        EtlConfigName = etlConfigName;
        
        return this;
    }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(string fieldName, string etlConfigName)
    {
        FieldName = fieldName;
        SourceQuantizationType = VectorEmbeddingType.Text;
        DestinationQuantizationType = Constants.VectorSearch.DefaultEmbeddingType;
        EtlConfigName = etlConfigName;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithEmbedding(string fieldName, VectorEmbeddingType storedEmbeddingQuantization)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithEmbedding(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithBase64(string fieldName, VectorEmbeddingType storedEmbeddingQuantization)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        IsBase64Encoded = true;

        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithBase64(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        IsBase64Encoded = true;

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
    /// <summary>
    /// Defines queried text.
    /// </summary>
    /// <param name="text">Queried text.</param>
    public void ByText(string text);
    
    /// <summary>
    /// Defines queried texts.
    /// </summary>
    /// <param name="texts">Queried texts.</param>
    public void ByText(IEnumerable<string> texts);
}

public interface IVectorEmbeddingFieldValueFactory
{
    /// <summary>
    /// Defines queried embedding.
    /// </summary>
    /// <param name="embedding">Enumerable containing embedding values.</param>
    public void ByEmbedding<T>(IEnumerable<T> embedding) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    ;
    
    /// <summary>
    /// Defines queried embeddings.
    /// </summary>
    /// <param name="embeddings">Enumerable containing embeddings values.</param>
    public void ByEmbedding<T>(IEnumerable<IEnumerable<T>> embeddings) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    ;
    
    /// <inheritdoc cref="ByEmbedding{T}(System.Collections.Generic.IEnumerable{T})"/>
    /// <param name="embedding">Array containing embedding values.</param>
    public void ByEmbedding<T>(T[] embedding) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    ;
    
    /// <inheritdoc cref="ByEmbedding{T}(System.Collections.Generic.IEnumerable{T})"/>
    /// <param name="embeddings">Array containing embeddings values.</param>
    public void ByEmbedding<T>(T[][] embeddings) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    ;
    
    /// <summary>
    /// Defines queried embedding in base64 format.
    /// </summary>
    /// <param name="base64Embedding">Embedding encoded as base64 string.</param>
    public void ByBase64(string base64Embedding);
    
    /// <summary>
    /// Defines queried embeddings in base64 format.
    /// </summary>
    /// <param name="base64Embeddings">Embeddings encoded as base64 strings.</param>
    public void ByBase64(IEnumerable<string> base64Embeddings);
    
    /// <summary>
    /// Defines queried embedding.
    /// </summary>
    /// <param name="embedding">RavenVector containing embedding values.</param>
    public void ByEmbedding<T>(RavenVector<T> embedding) where T : unmanaged
#if NET7_0_OR_GREATER
        , INumber<T>
#endif
    ;
}

public interface IVectorFieldValueFactory : IVectorEmbeddingTextFieldValueFactory, IVectorEmbeddingFieldValueFactory
{
    
}

public interface IVectorFieldValueFactoryAccessor
{
    internal object Embedding { get; set; }
    internal string Text { get; set; }
    internal IEnumerable<string> Texts { get; set; }
}

internal class VectorFieldValueFactory : IVectorFieldValueFactory, IVectorFieldValueFactoryAccessor
{
    public object Embedding { get; set; }
    public string Text { get; set; }
    public IEnumerable<string> Texts { get; set; }
    
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
    
    void IVectorEmbeddingFieldValueFactory.ByEmbedding<T>(IEnumerable<IEnumerable<T>> embeddings)
    {
        AssertEmbeddingType<T>();
        Embedding = embeddings;
    }
    
    void IVectorEmbeddingFieldValueFactory.ByEmbedding<T>(T[][] embeddings)
    {
        AssertEmbeddingType<T>();
        Embedding = embeddings;
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
        Text = base64Embedding;
    }
    
    void IVectorEmbeddingFieldValueFactory.ByBase64(IEnumerable<string> base64Embeddings)
    {
        Texts = base64Embeddings;
    }

    void IVectorEmbeddingTextFieldValueFactory.ByText(string text)
    {
        Text = text;
    }
    
    void IVectorEmbeddingTextFieldValueFactory.ByText(IEnumerable<string> texts)
    {
        Texts = texts;
    }
    
    void IVectorEmbeddingFieldValueFactory.ByEmbedding<T>(RavenVector<T> embedding)
    {
        Embedding = embedding;
    }

}
