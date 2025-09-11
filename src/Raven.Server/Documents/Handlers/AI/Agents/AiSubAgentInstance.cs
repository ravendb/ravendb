using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class AiSubAgentInstance : IDynamicJson
{
    public AiSubAgentInstance()
    {
        
    }

    public AiSubAgentInstance(string agent, string conversationId, string hash)
    {
        Agent = agent;
        ConversationId = conversationId;
        Hash = hash;
    }

    public string Agent;
    public string ConversationId;
    public string Hash;
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Agent)] = Agent,
            [nameof(ConversationId)] = ConversationId,
            [nameof(Hash)] = Hash
        };
    }
}

