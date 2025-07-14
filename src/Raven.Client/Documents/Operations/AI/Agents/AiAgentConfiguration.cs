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

    public AiAgentConfiguration(string name, string connectionStringName, string systemPrompt)
    {
        ValidationMethods.AssertNotNullOrEmpty(name, nameof(Name));
        ValidationMethods.AssertNotNullOrEmpty(connectionStringName, nameof(connectionStringName));
        ValidationMethods.AssertNotNullOrEmpty(systemPrompt, nameof(systemPrompt));

        Name = name;
        ConnectionStringName = connectionStringName;
        SystemPrompt = systemPrompt;
    }

    /// <summary>
    /// The identifier of the AI agent configuration.
    /// </summary>
    public string Identifier { get; set; }

    /// <summary>
    /// The name of the AI agent configuration.
    /// </summary>
    public string Name { get; set; }

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
    public List<AiAgentToolQuery> Queries { get; set; }= [];

    /// <summary>
    /// Model-side tools: callable actions where the AI agent fills parameters and invokes the tool as part of reasoning.
    /// The agent decides when to call them based on user input and context.
    /// When the agent calls them, it expects the user to provide "answers" for them.
    /// </summary>
    public List<AiAgentToolAction> Actions { get; set; } = [];

    /// <summary>
    /// Controls persistence behavior of chats - whether the chat history will be persistent or not
    /// </summary>
    public AiAgentPersistenceConfiguration Persistence { get; set; }

    /// <summary>
    /// Names of the required parameters that are used in the agent's queries and actions.
    /// Which has to be provided by the user each time we start a new chat.
    /// </summary>
    public HashSet<string> Parameters { get; set; } = new ();

    /// <summary>
    /// Configuration for reducing the chat messages list of the AI agent.
    /// </summary>
    /// <remarks>
    /// Defines how the chat messages list should be minimized before continuing the conversation,
    /// either by summarizing older messages into a compact prompt or by truncating them entirely.
    /// Only one reduction strategy (summarization or truncation) can be active at a time.
    /// </remarks>
    public AiAgentChatReductionConfiguration ChatReduction { get; set; }

    /// <summary>
    /// The maximum number of times the AI model can return tool call requests (responses that include <c>tool_calls</c>)
    /// for a single user prompt (e.g. when starting or resuming a chat).
    /// </summary>
    /// <value>
    /// Specifies the upper limit on how many tool-invocation responses
    /// the model is allowed to produce per individual user request.
    /// </value>
    public int MaxToolCallResponses { get; set; } = 16;

    internal AiAgentToolQuery FindQuery(string name)
    {
        foreach (AiAgentToolQuery query in Queries ?? [])
        {
            if(query.Name == name)
                return query;
        }

        return null;
    }
    
    internal AiAgentToolAction FindAction(string name)
    {
        foreach (AiAgentToolAction action in Actions ?? [])
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
            [nameof(Identifier)] = Identifier,
            [nameof(Name)] = Name,
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(SystemPrompt)] = SystemPrompt,
            [nameof(OutputSchema)] = OutputSchema,
            [nameof(SampleObject)] = SampleObject,
            [nameof(Queries)] = Queries != null ? new DynamicJsonArray(Queries) : null,
            [nameof(Actions)] = Actions != null ? new DynamicJsonArray(Actions) : null,
            [nameof(Persistence)] = Persistence?.ToJson(),
            [nameof(Parameters)] = new DynamicJsonArray(Parameters),
            [nameof(ChatReduction)] = ChatReduction?.ToJson(),
            [nameof(MaxToolCallResponses)] = MaxToolCallResponses
        };
    }
}
