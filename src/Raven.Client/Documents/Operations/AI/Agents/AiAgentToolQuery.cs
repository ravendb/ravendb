using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents
{
    /// <summary>
    /// Represents a query tool that can be invoked by an AI agent.
    /// The tool includes a name, description, query string, and parameter schema or sample object.
    /// When invoked by the AI model, the query is expected to be executed by the server (database),
    /// and its results provided back to the model.
    /// </summary>
    public class AiAgentToolQuery : IDynamicJson
    {
        public AiAgentToolQuery()
        {
            // for serialization
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AiAgentToolQuery"/> class with the specified properties.
        /// </summary>
        /// <param name="name">The identifier used by the AI to reference this specific query.</param>
        /// <param name="description">A human-readable description of what this query does or retrieves.</param>
        /// <param name="query">The actual query string (RQL) that represents this tool.</param>
        public AiAgentToolQuery(
            string name,
            string description,
            string query)
        {
            ValidationMethods.AssertNotNullOrEmpty(name, nameof(Name));
            ValidationMethods.AssertNotNullOrEmpty(description, nameof(Description));
            ValidationMethods.AssertNotNullOrEmpty(query, nameof(Query));

            Name = name;
            Description = description;
            Query = query;
        }

        /// <summary>
        /// The name of the tool query.
        /// This is the identifier used by the AI to reference this specific query.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The description of the tool query.
        /// Used by the AI to understand when to invoke this query.
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// The actual query string (RQL) that represents this tool.
        /// This query will be executed by the database when the model requests this tool.
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        /// A sample object representing the parameters for this tool.
        /// This should be a JSON-formatted string, showing an example of valid parameters.
        /// </summary>
        public string ParametersSampleObject { get; set; }

        /// <summary>
        /// The JSON schema for the parameters expected by this tool.
        /// This schema is used to validate and assist the AI in forming correct tool calls.
        /// </summary>
        public string ParametersSchema { get; set; }
        
        /// <summary>
        /// Options for the AI agent tool query.
        /// </summary>
        public AiAgentToolQueryOptions Options { get; set; }

        internal bool ShouldAddToInitialContext()
        {
            if (Options?.AddToInitialContext is null)
                return false;

            return Options.AddToInitialContext.Value;
        }

        internal bool ShouldAllowModelQueries()
        {
            if (Options?.AllowModelQueries is null)
                return true;

            return Options.AllowModelQueries.Value;
        }


        /// <summary>
        /// Serializes this query tool to a JSON structure.
        /// </summary>
        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Description)] = Description,
                [nameof(Query)] = Query,
                [nameof(ParametersSampleObject)] = ParametersSampleObject,
                [nameof(ParametersSchema)] = ParametersSchema
            };
            
            if (Options != null)
                djv[nameof(Options)] = Options.ToJson();

            return djv;
        }
    }

}
