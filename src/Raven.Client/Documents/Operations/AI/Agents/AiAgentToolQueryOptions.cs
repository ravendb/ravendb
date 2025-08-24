using System;

namespace Raven.Client.Documents.Operations.AI.Agents;

[Flags]
public enum AiAgentToolQueryOptions
{
    /// <summary>
    /// Allows the model to execute the query when it needs to, based on the model's own judgement.
    /// This is the default behavior
    /// </summary>
    AllowModelQueries,

    /// <summary>
    /// The associated query results will be sent to the model as part of the initial context.
    ///
    /// This allows us to speed up the initial response, if it is expected that the model will usually need to
    /// call this query to do its work.
    /// 
    /// Requires that the query has no model-supplied parameters (but can use agent scope parameters).
    /// 
    /// If only <see cref="AddToInitialContext"/> is specified, this query will run *only* during initial context
    /// and won't be callable by the model afterward.
    ///
    /// If <see cref="AllowModelQueries"/> is ORed with this value, then the query run both on initial context and
    /// the model can call it afterward as well. That can be useful if the model want to see fresh changes since
    /// the conversation started.
    /// </summary>
    AddToInitialContext,
}
