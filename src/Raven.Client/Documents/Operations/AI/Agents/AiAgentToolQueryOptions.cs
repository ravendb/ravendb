using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Tool query execution options.
/// </summary>
public class AiAgentToolQueryOptions : IDynamicJson
{
    /// <summary>
    /// When true, the model is allowed to execute this query on demand based on its own judgment.
    /// When false, the model cannot call this query (unless executed as part of initial context).
    /// When null, server-side defaults apply.
    /// </summary>
    public bool? AllowModelQueries { get; set; }

    /// <summary>
    /// When true, the query will be executed during the initial context build and its results provided to the model.
    /// When false, the query will not be executed for the initial context.
    /// When null, server-side defaults apply.
    ///
    /// Notes:
    /// - The query must not require model-supplied parameters (it may use agent-scope parameters).
    /// - If only <see cref="AddToInitialContext"/> is true and <see cref="AllowModelQueries"/> is false, the query runs only during the initial context and is not callable by the model afterward.
    /// - If both <see cref="AddToInitialContext"/> and <see cref="AllowModelQueries"/> are true, the query will run during the initial context and may also be invoked later by the model (e.g., to fetch fresh data).
    /// </summary>
    public bool? AddToInitialContext { get; set; }

    /// <summary>
    /// Serializes the tool query options to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(AllowModelQueries)] = AllowModelQueries,
            [nameof(AddToInitialContext)] = AddToInitialContext
        };
    }
}
