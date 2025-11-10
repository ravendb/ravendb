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

        private ExceededTokenThresholdDetails(string agentName, string conversationId, long tokenCount, long tokenLimit, List<ToolCallDetails> toolCalls, string recommendation)
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
        public long TokenCount { get; set; }
        public long TokenThreshold { get; set; }
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

        public static ExceededTokenThresholdDetails Add(DatabaseNotificationCenter notificationCenter, string agentName, string conversationId, long tokenCount, long tokenThreshold, List<ToolCallDetails> toolCalls)
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

            return details;
        }

        public static AlertRaised CreateAlert(ExceededTokenThresholdDetails pendingAlertDetails, string databaseName)
        {
            var configurationKey = RavenConfiguration.GetKey(x => x.Ai.ToolsTokenUsageThreshold);
            var msg = $"In conversation '{pendingAlertDetails.ConversationId}', the AI Agent '{pendingAlertDetails.AgentName}' sent a request with {pendingAlertDetails.TokenCount} tokens to the LLM, exceeding the configured threshold of {pendingAlertDetails.TokenThreshold} tokens. " +
                      $"You can adjust the limit by setting the '{configurationKey}' configuration value";
            return AlertRaised.Create(databaseName, $"AI Agent '{pendingAlertDetails.AgentName}': Exceeded Token Threshold",
                msg, AlertReason.AiAgent_ExceededTokenThreshold, NotificationSeverity.Warning, details: pendingAlertDetails);
        }

        public class ToolCallDetails : IDynamicJson
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public ToolType Type { get; set; }
            public string Arguments { get; set; }
            public string Query { get; set; }

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(Id)] = Id,
                    [nameof(Name)] = Name,
                    [nameof(Type)] = Type.ToString(),
                    [nameof(Arguments)] = Arguments
                };
                if (Type == ToolType.Query)
                {
                    djv[nameof(Query)] = Query;
                }

                return djv;
            }
        }
    }
}
