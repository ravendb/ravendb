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
    /// <param name="vectorIndexingStrategy">Name of the vector search technique used for finding similar documents.</param>
    public IVectorEmbeddingTextField WithText(string fieldName);
    
    /// <inheritdoc cref="WithText(string,Raven.Client.Documents.Indexes.Vector.VectorIndexingStrategy)"/>
    /// <param name="propertySelector">Path to the document field containing text data.</param>
    public IVectorEmbeddingTextField WithText(Expression<Func<T, object>> propertySelector);
    
    /// <summary>
    /// Defines the embedding field that vector search will be performed on.
    /// </summary>
    /// <param name="fieldName">Name of the document field containing embedding data.</param>
    /// <param name="storedEmbeddingQuantization">Quantization that was performed on stored embeddings.</param>
    /// <param name="vectorIndexingStrategy">Name of the vector search technique used for finding similar documents.</param>
    public IVectorEmbeddingField WithEmbedding(string fieldName, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType);
    
    ///<inheritdoc cref="WithEmbedding(string,Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType,Raven.Client.Documents.Indexes.Vector.VectorIndexingStrategy)"/>
    /// <param name="propertySelector">Path to the document field containing embedding data.</param>
    public IVectorEmbeddingField WithEmbedding(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType);
    
    /// <summary>
    /// Defines the embedding field (encoded as base64) that vector search will be performed on.
    /// </summary>
    /// <param name="fieldName">Name of the document field containing base64 encoded embedding data.</param>
    /// <param name="storedEmbeddingQuantization">Quantization of stored embeddings.</param>
    /// <param name="vectorIndexingStrategy">Name of the vector search technique used for finding similar documents.</param>
    public IVectorEmbeddingField WithBase64(string fieldName, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType);
    
    /// <inheritdoc cref="WithBase64(string,Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType,Raven.Client.Documents.Indexes.Vector.VectorIndexingStrategy)"/>
    public IVectorEmbeddingField WithBase64(Expression<Func<T, object>> propertySelector, VectorEmbeddingType storedEmbeddingQuantization = Constants.VectorSearch.DefaultEmbeddingType);

    /// <summary>
    /// Defines the field (that's already indexed) that vector search will be performed on.
    /// </summary>
    /// <param name="fieldName">Name of the document field containing indexed data.</param>
    public IVectorField WithField(string fieldName);
    
    /// <inheritdoc cref="WithField(string)"/>
    /// <param name="propertySelector">Path to the document field containing indexed data.</param>
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

internal sealed class VectorEmbeddingFieldFactory<T> : IVectorFieldFactory<T>, IVectorField, IVectorEmbeddingField, IVectorEmbeddingTextField
{
    private bool _byFieldMethodUsed;

    internal string FieldName { get; set; }
    internal VectorEmbeddingType SourceQuantizationType { get; set; } = Constants.VectorSearch.DefaultEmbeddingType;
    internal VectorEmbeddingType DestinationQuantizationType { get; set; } = Constants.VectorSearch.DefaultEmbeddingType;
    internal bool IsBase64Encoded { get; set; }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(Expression<Func<T, object>> propertySelector)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = VectorEmbeddingType.Text;
        DestinationQuantizationType = Constants.VectorSearch.DefaultEmbeddingType;
        
        return this;
    }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(string fieldName)
    {
        FieldName = fieldName;
        SourceQuantizationType = VectorEmbeddingType.Text;
        DestinationQuantizationType = Constants.VectorSearch.DefaultEmbeddingType;
        
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
    
    /// <inheritdoc cref="ByEmbedding{T}(System.Collections.Generic.IEnumerable{T})"/>
    /// <param name="embedding">Array containing embedding values.</param>
    public void ByEmbedding<T>(T[] embedding) where T : unmanaged
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
    
    void IVectorEmbeddingFieldValueFactory.ByEmbedding<T>(RavenVector<T> embedding)
    {
        Embedding = embedding;
    }

}
