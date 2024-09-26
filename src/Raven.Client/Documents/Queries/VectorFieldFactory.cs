using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries;

public interface IVectorFieldFactory<T>
{
    public IVectorEmbeddingTextField WithText(string fieldName);
    
    public IVectorEmbeddingTextField WithText(Expression<Func<T, string>> propertySelector);
    
    public IVectorEmbeddingField WithEmbedding(string fieldName, EmbeddingQuantizationType storedEmbeddingQuantization = EmbeddingQuantizationType.None);
    
    public IVectorEmbeddingField WithEmbedding(Expression<Func<T, string>> propertySelector, EmbeddingQuantizationType storedEmbeddingQuantization = EmbeddingQuantizationType.None);
    public IVectorEmbeddingField WithBase64(string fieldName, EmbeddingQuantizationType storedEmbeddingQuantization = EmbeddingQuantizationType.None);
    
    public IVectorEmbeddingField WithBase64(Expression<Func<T, string>> propertySelector, EmbeddingQuantizationType storedEmbeddingQuantization = EmbeddingQuantizationType.None);
}

public interface IVectorEmbeddingTextField
{
    
}

public interface IVectorEmbeddingField
{
    public IVectorEmbeddingField TargetQuantization(EmbeddingQuantizationType targetEmbeddingQuantization);
}

internal sealed class VectorEmbeddingFieldFactory<T> : IVectorFieldFactory<T>, IVectorEmbeddingTextField, IVectorEmbeddingField
{
    internal string FieldName { get; set; }
    internal EmbeddingQuantizationType SourceQuantizationType { get; set; }
    internal EmbeddingQuantizationType DestinationQuantizationType { get; set; }
    internal bool IsBase64Encoded { get; set; }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(Expression<Func<T, string>> propertySelector)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);

        return this;
    }
    
    IVectorEmbeddingTextField IVectorFieldFactory<T>.WithText(string fieldName)
    {
        FieldName = fieldName;

        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithEmbedding(string fieldName, EmbeddingQuantizationType storedEmbeddingQuantization)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithEmbedding(Expression<Func<T, string>> propertySelector, EmbeddingQuantizationType storedEmbeddingQuantization)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        
        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithBase64(string fieldName, EmbeddingQuantizationType storedEmbeddingQuantization)
    {
        FieldName = fieldName;
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        IsBase64Encoded = true;

        return this;
    }

    IVectorEmbeddingField IVectorFieldFactory<T>.WithBase64(Expression<Func<T, string>> propertySelector, EmbeddingQuantizationType storedEmbeddingQuantization)
    {
        FieldName = propertySelector.ToPropertyPath(DocumentConventions.Default);
        SourceQuantizationType = storedEmbeddingQuantization;
        DestinationQuantizationType = SourceQuantizationType;
        IsBase64Encoded = true;

        return this;
    }
    
    IVectorEmbeddingField IVectorEmbeddingField.TargetQuantization(EmbeddingQuantizationType targetEmbeddingQuantization)
    {
        DestinationQuantizationType = targetEmbeddingQuantization;
        
        if (DestinationQuantizationType < SourceQuantizationType)
            throw new Exception($"Cannot quantize vector with {SourceQuantizationType} quantization into {DestinationQuantizationType}");

        if (SourceQuantizationType == EmbeddingQuantizationType.I8 && DestinationQuantizationType == EmbeddingQuantizationType.I1)
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
    public void ByEmbedding(float[] embedding, EmbeddingQuantizationType queriedEmbeddingQuantization = EmbeddingQuantizationType.None);
    public void ByBase64(string base64Embedding, EmbeddingQuantizationType queriedEmbeddingQuantization = EmbeddingQuantizationType.None);
}

public interface IVectorEmbeddingFieldValueFactoryBase : IVectorEmbeddingTextFieldValueFactory, IVectorEmbeddingFieldValueFactory
{
    
}

internal class VectorEmbeddingValueFactory : IVectorEmbeddingFieldValueFactoryBase
{
    public float[] Embedding { get; set; }
    public string Text { get; set; }
    public string Base64Embedding { get; set; }
    public EmbeddingQuantizationType EmbeddingQuantizationType { get; set; }
    
    void IVectorEmbeddingFieldValueFactory.ByEmbedding(float[] embedding, EmbeddingQuantizationType queriedEmbeddingQuantization)
    {
        Embedding = embedding;
        EmbeddingQuantizationType = queriedEmbeddingQuantization;
    }

    void IVectorEmbeddingFieldValueFactory.ByBase64(string base64Embedding, EmbeddingQuantizationType queriedEmbeddingQuantization)
    {
        Base64Embedding = base64Embedding;
        EmbeddingQuantizationType = queriedEmbeddingQuantization;
    }

    void IVectorEmbeddingTextFieldValueFactory.ByText(string text)
    {
        Text = text;
    }
}

public enum EmbeddingQuantizationType
{
    None = 0,
    F32 = None,
    I8 = 1,
    I1 = 2
}
