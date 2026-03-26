using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Describes a JavaScript transformation used to perform GenAI processing (chat/completions) on documents.
/// </summary>
public class GenAiTransformation
{
    /// <summary>
    /// The JavaScript script that must call <c>ai.genContext(ctx)</c> to set up the GenAI context.
    /// </summary>
    public string Script { get; set; }

    /// <summary>
    /// Validates that the script calls <c>ai.genContext</c>.
    /// </summary>
    /// <param name="error">Receives an explanatory error message when the script is invalid.</param>
    /// <returns><c>true</c> if the script is valid; otherwise, <c>false</c>.</returns>
    public bool ValidateScript(out string error)
    {
        error = string.Empty;
        if (Script.Contains("ai.genContext")) 
            return true;
        error = "You must call the ai.genContext(ctx) function in your script";
        return false;

    }

    /// <summary>
    /// Serializes the transformation to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson() => new()
    {
        [nameof(Script)] = Script
    };
}
