#nullable enable
using System.Text;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

/// <summary>
/// The configuration for the ONNX model.
/// </summary>
public sealed class OnnxSettings
{
    /// <summary>
    /// The flag to indicate whether the model should be case-sensitive.
    /// </summary>
    public bool? CaseSensitive { get; set; }

    /// <summary>The maximum number of tokens that the model can process.</summary>
    public int? MaximumTokens { get; set; }

    /// <summary>Gets or sets the cls token. Defaults to "[CLS]".</summary>
    /// <remarks>
    /// The CLS token is a special token that is added to the beginning of the input sequence.
    /// It is used to represent the classification of the entire input sequence.
    /// </remarks>
    public string? ClsToken { get; set; }

    /// <summary>Gets or sets the unknown token.</summary>
    /// <remarks>
    /// The UNK token is a special token that is used to represent unknown words in the input sequence.
    /// It is used to handle out-of-vocabulary words.
    /// </remarks>
    public string? UnknownToken { get; set; }

    /// <summary>Gets or sets the sep token.</summary>
    /// <remarks>
    /// The SEP token is a special token that is added to the end of the input sequence.
    /// It is used to separate the input sequence from the classification label.
    /// </remarks>
    public string? SepToken { get; set; }

    /// <summary>Gets or sets the pad token.</summary>
    /// <remarks>
    /// The PAD token is a special token that is used to pad the input sequence to a fixed length.
    /// It is used to handle input sequences that are shorter than the maximum sequence length.
    /// </remarks>
    public string? PadToken { get; set; }

    /// <summary>Gets or sets the type of Unicode normalization to perform on input text.</summary>
    /// <remarks>
    /// Unicode normalization is the process of transforming input text into a standard form that can be more easily compared.
    /// The normalization form determines the specific normalization rules that are applied to the input text.
    /// </remarks>
    public NormalizationForm? UnicodeNormalization { get; set; }

    /// <summary>Gets or sets the pooling mode to use when generating the fixed-length embedding result.</summary>
    public OnnxEmbeddingPoolingMode? PoolingMode { get; set; }

    /// <summary>Gets or sets whether the resulting embedding vectors should be explicitly normalized.</summary>
    /// <remarks>Normalized embeddings may be compared more efficiently, such as by using a dot product rather than cosine similarity.</remarks>
    public bool? NormalizeEmbeddings { get; set; }

    public bool HasSettings() => true;

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(CaseSensitive)] = CaseSensitive,
            [nameof(MaximumTokens)] = MaximumTokens,
            [nameof(ClsToken)] = ClsToken,
            [nameof(UnknownToken)] = UnknownToken,
            [nameof(SepToken)] = SepToken,
            [nameof(PadToken)] = PadToken,
            [nameof(UnicodeNormalization)] = UnicodeNormalization.ToString(),
            [nameof(PoolingMode)] = PoolingMode.ToString(),
            [nameof(NormalizeEmbeddings)] = NormalizeEmbeddings
        };
}

public enum OnnxEmbeddingPoolingMode
{
    /// <summary>Uses the maximum across all token embeddings.</summary>
    Max,

    /// <summary>Calculates the average across all token embeddings.</summary>
    Mean,

    /// <summary>Calculates the average across all token embeddings, divided by the square root of the number of tokens.</summary>
    MeanSquareRootTokensLength,
}
