using System.Text;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

/// <summary>
/// The configuration for the ONNX model.
/// </summary>
public sealed class OnnxSettings
{
    /// <summary>
    /// The path to the ONNX model file.
    /// </summary>
    public string ModelPath { get; set; }

    /// <summary>
    /// The path to the vocab file.
    /// </summary>
    public string VocabularyPath { get; set; }

    /// <summary>
    /// The flag to indicate whether the model should be case-sensitive. Defaults to false.
    /// </summary>
    public bool CaseSensitive { get; set; } = false;

    /// <summary>The maximum number of tokens that the model can process. Defaults to 512.</summary>
    public int MaximumTokens { get; set; } = 512;

    /// <summary>Gets or sets the cls token. Defaults to "[CLS]".</summary>
    /// <remarks>
    /// The CLS token is a special token that is added to the beginning of the input sequence.
    /// It is used to represent the classification of the entire input sequence.
    /// </remarks>
    public string ClsToken { get; set; } = "[CLS]";

    /// <summary>Gets or sets the unknown token. Defaults to "[UNK]".</summary>
    /// <remarks>
    /// The UNK token is a special token that is used to represent unknown words in the input sequence.
    /// It is used to handle out-of-vocabulary words.
    /// </remarks>
    public string UnknownToken { get; set; } = "[UNK]";

    /// <summary>Gets or sets the sep token. Defaults to "[SEP]".</summary>
    /// <remarks>
    /// The SEP token is a special token that is added to the end of the input sequence.
    /// It is used to separate the input sequence from the classification label.
    /// </remarks>
    public string SepToken { get; set; } = "[SEP]";

    /// <summary>Gets or sets the pad token. Defaults to "[PAD]".</summary>
    /// <remarks>
    /// The PAD token is a special token that is used to pad the input sequence to a fixed length.
    /// It is used to handle input sequences that are shorter than the maximum sequence length.
    /// </remarks>
    public string PadToken { get; set; } = "[PAD]";

    /// <summary>Gets or sets the type of Unicode normalization to perform on input text. Defaults to <see cref="NormalizationForm.FormD"/>.</summary>
    /// <remarks>
    /// Unicode normalization is the process of transforming input text into a standard form that can be more easily compared.
    /// The normalization form determines the specific normalization rules that are applied to the input text.
    /// </remarks>
    public NormalizationForm UnicodeNormalization { get; set; } = NormalizationForm.FormD;

    /// <summary>Gets or sets the pooling mode to use when generating the fixed-length embedding result. Defaults to <see cref="EmbeddingPoolingMode.Mean"/>.</summary>
    public OnnxEmbeddingPoolingMode PoolingMode { get; set; } = OnnxEmbeddingPoolingMode.Mean;

    /// <summary>Gets or sets whether the resulting embedding vectors should be explicitly normalized. Defaults to false.</summary>
    /// <remarks>Normalized embeddings may be compared more efficiently, such as by using a dot product rather than cosine similarity.</remarks>
    public bool NormalizeEmbeddings { get; set; } = false;

    public bool HasSettings() => string.IsNullOrWhiteSpace(ModelPath) == false && string.IsNullOrWhiteSpace(VocabularyPath) == false;

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(ModelPath)] = ModelPath,
            [nameof(VocabularyPath)] = VocabularyPath,
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
