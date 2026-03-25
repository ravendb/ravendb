using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    /// Initializes a new instance of <see cref="AiAgentConfiguration"/> with the specified name, connection string, and system prompt.
    /// </summary>
    /// <param name="name">The name of the AI agent configuration.</param>
    /// <param name="connectionStringName">The name of the connection string used to connect to the AI provider.</param>
    /// <param name="systemPrompt">The prompt that guides the behavior and purpose of the AI agent.</param>
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
    /// Server side sub-agents that the model can also call. Those sub-agents will be invoked and managed as part of the
    /// agent run, including running their own queries, etc. Parameters for the sub-agents will be inherited from the
    /// root agent.
    ///
    /// Handle("attendance-agent/SendEmail", ...);
    /// Handle("benefits-agent/SendEmail", ...);
    /// Handle("benefits-agent/friendly-agent/SendEmail", ...);
    /// 
    /// If there is an action defined in the sub-agent, it will return all the way to the client code for handling.
    /// </summary>
    public List<AiAgentToolSubAgent> SubAgents { get; set; } = [];

    /// <summary>
    /// The required parameters that are used in the agent's queries and actions.
    /// Which has to be provided by the user each time we start a new chat.
    /// </summary>
    public List<AiAgentParameter> Parameters { get; set; } = new ();

    /// <summary>
    /// Configuration for reducing the chat messages list of the AI agent.
    /// </summary>
    /// <remarks>
    /// Defines how the chat messages list should be minimized before continuing the conversation,
    /// either by summarizing older messages into a compact prompt or by truncating them entirely.
    /// Only one reduction strategy (summarization or truncation) can be active at a time.
    /// </remarks>
    public AiAgentChatTrimmingConfiguration ChatTrimming { get; set; } = 
        new(new AiAgentSummarizationByTokens());

    /// <summary>
    /// The maximum number of times the AI model can return tool call requests (responses that include <c>tool_calls</c>)
    /// for a single user prompt (e.g. when starting or resuming a chat).
    /// </summary>
    /// <value>
    /// Specifies the upper limit on how many tool-invocation responses
    /// the model is allowed to produce per individual user request.
    /// </value>
    public int? MaxModelIterationsPerCall { get; set; }

    /// <summary>
    /// Indicates whether the ai-agent is disabled.
    /// </summary>
    public bool Disabled { get; set; }


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

    internal AiAgentToolSubAgent FindSubAgent(string identifier)
    {
        if (SubAgents?.Count > 0 == false)
            return null;

        foreach (AiAgentToolSubAgent tool in SubAgents)
        {
            if (tool.Identifier == identifier)
                return tool;
        }

        return null;
    }

    /// <summary>
    /// Serializes the configuration to a JSON structure.
    /// </summary>
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
            [nameof(SubAgents)] = SubAgents != null ? new DynamicJsonArray(SubAgents) : null,
            [nameof(Parameters)] = new DynamicJsonArray(Parameters),
            [nameof(ChatTrimming)] = ChatTrimming?.ToJson(),
            [nameof(MaxModelIterationsPerCall)] = MaxModelIterationsPerCall,
            [nameof(Disabled)] = Disabled,
        };
    }

    public DynamicJsonValue ToAuditJson()
    {
        return ToJson();
    }

    internal void AppendCapabilities(StringBuilder sb)
    {
        sb.AppendLine("Capabilities:");
        foreach (var q in Queries ?? [])
        {
            sb.Append("- ").Append(q.Name).Append(" - ").AppendLine(q.Description);
        }
        foreach (var q in SubAgents ?? [])
        {
            sb.Append("- ").Append(q.Identifier).Append(" - ").AppendLine(q.Description);
        }
        foreach (var q in Actions ?? [])
        {
            sb.Append("- ").Append(q.Name).Append(" - ").AppendLine(q.Description);
        }
    }
}
