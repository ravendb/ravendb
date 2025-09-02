using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Server.Config;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class ExceededTokenThresholdDetails : INotificationDetails
    {
        private ExceededTokenThresholdDetails() { }

        private ExceededTokenThresholdDetails(string agentName, string conversationId, int tokenCount, int tokenLimit, List<ToolCallDetails> toolCalls, string recommendation)
        {
            AgentName = agentName;
            ConversationId = conversationId;
            TokenCount = tokenCount;
            TokenThreshold = tokenLimit;
            ToolCalls = toolCalls;
            Recommendation = recommendation;
        }

        public string AgentName { get; set; }
        public string ConversationId { get; set; }
        public int TokenCount { get; set; }
        public int TokenThreshold { get; set; }
        public List<ToolCallDetails> ToolCalls { get; set; }
        public string Recommendation { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(AgentName)] = AgentName,
                [nameof(ConversationId)] = ConversationId,
                [nameof(TokenCount)] = TokenCount,
                [nameof(TokenThreshold)] = TokenThreshold,
                [nameof(ToolCalls)] = new DynamicJsonArray(ToolCalls.Select(tc => tc.ToJson())),
                [nameof(Recommendation)] = Recommendation
            };
        }

        public static void Add(DatabaseNotificationCenter notificationCenter, string agentName, string conversationId, int tokenCount, int tokenThreshold, List<ToolCallDetails> toolCalls)
        {
            var configurationKey = RavenConfiguration.GetKey(x => x.Ai.ToolsTokenUsageThreshold);
            var message = $"In conversation '{conversationId}', the AI Agent '{agentName}' sent a request with {tokenCount} tokens to the LLM, exceeding the configured threshold of {tokenThreshold} tokens. " +
                          $"You can adjust the limit by setting the '{configurationKey}' configuration value";

            var hasActionTools = toolCalls.Any(tc => tc.Type == ToolType.Action);
            var hasQueryTools = toolCalls.Any(tc => tc.Type == ToolType.Query);

            var recommendationBuilder = new StringBuilder();
            if (hasActionTools)
            {
                recommendationBuilder.Append("For Action Tools: Consider making the content shorter or use a format that will reduce the total tokens count");
            }

            if (hasQueryTools)
            {
                if (recommendationBuilder.Length > 0)
                {
                    recommendationBuilder.Append(" | ");
                }
                recommendationBuilder.Append("For Query Tools: Consider using a limit or select fewer fields in your query from your documents to reduce the total size.");
            }

            var details = new ExceededTokenThresholdDetails
            (
                agentName,
                conversationId,
                tokenCount,
                tokenThreshold,
                toolCalls,
                recommendationBuilder.ToString()
                );

            var alert = AlertRaised.Create(
                notificationCenter.Database,
                $"AI Agent '{agentName}': Exceeded Token Threshold",
                message,
                AlertType.AiAgent_ExceededTokenThreshold,
                NotificationSeverity.Warning,
                details: details);

            notificationCenter.Add(alert);
        }

        public class ToolCallDetails : IDynamicJson
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public ToolType Type { get; set; }
            public string Arguments { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Id)] = Id,
                    [nameof(Name)] = Name,
                    [nameof(Type)] = Type.ToString(),
                    [nameof(Arguments)] = Arguments
                };
            }
        }
    }
}
