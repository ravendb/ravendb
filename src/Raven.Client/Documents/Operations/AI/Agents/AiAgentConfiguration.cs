using System;
using System.Collections.Generic;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Defines the configuration for an AI agent in RavenDB, including the system prompt,
/// tools (queries/actions), output schema, persistence settings, and connection string.
/// </summary>
public class AiAgentConfiguration : IDynamicJson
{
    public AiAgentConfiguration()
    {
        // for serialization purposes
    }

    /// <summary>
    /// Initializes a new instance of <see cref="AiAgentConfiguration"/> with the specified connection string and system prompt.
    /// </summary>
    /// <param name="connectionStringName">The name of the connection string to use for the AI Agent.</param>
    /// <param name="systemPrompt">The system prompt that defines the agent’s role and behavior.</param>
    public AiAgentConfiguration(string connectionStringName, string systemPrompt)
    {
        ValidationMethods.AssertNotNullOrEmpty(connectionStringName, nameof(connectionStringName));
        ValidationMethods.AssertNotNullOrEmpty(systemPrompt, nameof(systemPrompt));
        
        ConnectionStringName = connectionStringName;
        SystemPrompt = systemPrompt;
    }

    /// <summary>
    /// The name of the connection string used to connect to the AI provider.
    /// </summary>
    public string ConnectionStringName { get; set; }

    /// <summary>
    /// The prompt that guides the behavior and purpose of the AI agent.
    /// </summary>
    public string SystemPrompt { get; set; }

    /// <summary>
    /// A sample object (as string) describing an example for an AI agent's output.
    /// This allows validation and parsing of the AI-generated response according to a known format.
    ///
    /// For example:
    /// <code>
    /// {
    ///   "Answer": "Answer to the user question",
    ///   "Relevant": true,
    ///   "RelevantOrdersId": ["The order ids relevant to the query or response"],
    ///   "MatchingProductsId": ["All the product ids referenced either by the user or the system"]
    /// }
    /// </code>
    /// </summary>
    public string SampleObject { get; set; }

    /// <summary>
    /// A JSON schema describing the expected structure of the AI agent's output.
    /// This allows validation and parsing of the AI-generated response according to a known format.
    /// </summary>
    public string OutputSchema { get; set; }

    /// <summary>
    /// Database-side tools: predefined queries that RavenDB executes to fetch data directly during chat.
    /// The agent decides when to call them based on user input and context.
    /// When the agent calls them, it gets an actual data from the database based on these queries.
    /// </summary>
    public List<ToolQuery> Queries { get; set; }= [];

    /// <summary>
    /// Model-side tools: callable actions where the AI agent fills parameters and invokes the tool as part of reasoning.
    /// The agent decides when to call them based on user input and context.
    /// When the agent calls them, it expects the user to provide "answers" for them.
    /// </summary>
    public List<ToolAction> Actions { get; set; } = [];

    /// <summary>
    /// Controls persistence behavior of chats - whether the chat history will be persistent or not
    /// </summary>
    public PersistenceConfiguration Persistence { get; set; }

    /// <summary>
    /// Names of the required parameters that are used in the agent's queries and actions.
    /// Which has to be provided by the user each time we start a new chat.
    /// </summary>
    public HashSet<string> Parameters { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    public class PersistenceConfiguration : IDynamicJson
    {
        public string Collection { get; set; }
        public TimeSpan? Expires { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Collection)] = Collection,
                [nameof(Expires)] = Expires?.TotalMilliseconds
            };
        }
    }
    
    public class ToolAction : IDynamicJson
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string ParametersSampleObject { get; set; }
        public string ParametersSchema { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Description)] = Description,
                [nameof(ParametersSampleObject)] = ParametersSampleObject,
                [nameof(ParametersSchema)] = ParametersSchema
            };
        }
    }

    public class ToolQuery : IDynamicJson
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string Query { get; set; }
        public string ParametersSampleObject { get; set; }
        public string ParametersSchema { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Description)] = Description,
                [nameof(Query)] = Query,
                [nameof(ParametersSampleObject)] = ParametersSampleObject,
                [nameof(ParametersSchema)] = ParametersSchema
            };
        }
    }

    internal ToolQuery FindQuery(string name)
    {
        foreach (ToolQuery query in Queries ?? [])
        {
            if(query.Name == name)
                return query;
        }

        return null;
    }
    internal ToolAction FindAction(string name)
    {
        foreach (ToolAction action in Actions ?? [])
        {
            if(action.Name == name)
                return action;
        }

        return null;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(SystemPrompt)] = SystemPrompt,
            [nameof(OutputSchema)] = OutputSchema,
            [nameof(SampleObject)] = SampleObject,
            [nameof(Queries)] = Queries != null ? new DynamicJsonArray(Queries) : null,
            [nameof(Actions)] = Actions != null ? new DynamicJsonArray(Actions) : null,
            [nameof(Persistence)] = Persistence?.ToJson(),
            [nameof(Parameters)] = new DynamicJsonArray(Parameters)
        };
    }
}
