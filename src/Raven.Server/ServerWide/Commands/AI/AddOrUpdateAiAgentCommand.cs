using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.ServerWide.Commands.AI
{
    public class AddOrUpdateAiAgentCommand : UpdateDatabaseCommand
    {
        public AiAgentConfiguration Configuration;

        public AddOrUpdateAiAgentCommand()
        {
            // for deserialization    
        }

        public AddOrUpdateAiAgentCommand(string database, AiAgentConfiguration configuration, string uniqueRequestId) : base(database, uniqueRequestId)
        {
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration)); 
        }

        public override void Initialize(ServerStore serverStore, ClusterOperationContext context)
        {
            try
            {
                ValidateConfiguration(context, Configuration);
            }
            catch (Exception e)
            {
                throw new RachisApplyException($"Failed to validate AI Agent configuration for '{Configuration.Name}' with identifier '{Configuration.Identifier}'", e);
            }
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (string.IsNullOrEmpty(Configuration.Identifier))
                throw new RachisApplyException("Ai Agent Identifier cannot be empty");

            if (string.IsNullOrEmpty(Configuration.Name))
                throw new RachisApplyException("Ai Agent Name cannot be empty");

            record.AiAgents ??= [];

            // validate it either doesn't exist (CREATE), or it existed with both the same identifier and the same name (UPDATE).
            var isUpdate = Validate(record);

            if (isUpdate)
                record.AiAgents.RemoveAll(c => c.Identifier == Configuration.Identifier);
            else
                EnsureTaskNameIsNotUsed(record, Configuration.Name);

            record.AiAgents.Add(Configuration);
        }

        private bool Validate(DatabaseRecord databaseRecord)
        {
            if (databaseRecord == null)
                throw new RachisApplyException("Failed to get database record, but it is required for further validation");

            if (string.IsNullOrWhiteSpace(Configuration.Identifier))
                throw new RachisApplyException("Ai Agent configuration identifier identifier must be set, but it is not");

            if (AiTaskIdentifierHelper.ValidateIdentifier(Configuration.Identifier, out var errors) == false)
                throw new RachisApplyException($"Invalid identifier format. Validation errors:{Environment.NewLine} - {string.Join($"{Environment.NewLine} - ", errors)}");

            var isUpdate = databaseRecord.AiAgents.Any(x => x.Identifier == Configuration.Identifier);

            var identifierConflicts = databaseRecord.AiAgents
                .Where(x => x.Identifier == Configuration.Identifier && x.Name != Configuration.Name)
                .ToArray();

            if (identifierConflicts.Length > 0)
                throw new RachisApplyException(
                    $"Can't {(isUpdate ? "update" : "create")} AI Agent config: '{Configuration.Name}'. " +
                    $"The identifier '{Configuration.Identifier}' is already used by " +
                    $"AI Agent config{(identifierConflicts.Length > 1 ? "s" : "")} " +
                    $"'{string.Join("', '", identifierConflicts.Select(x => x.Name))}'");

            return isUpdate;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = Configuration.ToJson();
        }

        private static readonly Regex ToolNameChecker = new("^[a-zA-Z0-9_-]+$", RegexOptions.Compiled);

        public static void ValidateConfiguration(JsonOperationContext context, AiAgentConfiguration configuration)
        {
            var reduction = configuration.ChatTrimming;
            if (reduction != null)
            {
                if ((reduction.Tokens != null) == (reduction.Truncate != null))
                {
                    throw new InvalidOperationException($"{nameof(configuration.ChatTrimming)} requires exactly one strategy: " +
                                                        $"either {nameof(reduction.Tokens)} or {nameof(reduction.Truncate)}, " +
                                                        "but not both or neither.");
                }

                if (reduction.Truncate != null)
                {
                    var after = reduction.Truncate.MessagesLengthAfterTruncate;
                    var before = reduction.Truncate.MessagesLengthBeforeTruncate;
                    if (after > before)
                        throw new InvalidOperationException(
                            $"{nameof(reduction.Truncate.MessagesLengthAfterTruncate)} ({after}) must be less of equal then {nameof(reduction.Truncate.MessagesLengthBeforeTruncate)} ({before})");

                    if (after <= 0)
                        throw new InvalidOperationException(
                            $"{nameof(reduction.Truncate.MessagesLengthAfterTruncate)} ({after}) must be greater then 0");

                    if (before <= 0)
                        throw new InvalidOperationException(
                            $"{nameof(reduction.Truncate.MessagesLengthBeforeTruncate)} ({before}) must be greater then 0");
                }

                if (reduction.Tokens != null)
                {
                    if (string.IsNullOrEmpty(reduction.Tokens.SummarizationTaskBeginningPrompt))
                        throw new InvalidOperationException(
                            $"{nameof(reduction.Tokens.SummarizationTaskBeginningPrompt)} cannot be empty when {nameof(reduction.Tokens)} is used");

                    if (string.IsNullOrEmpty(reduction.Tokens.SummarizationTaskEndPrompt))
                        throw new InvalidOperationException(
                            $"{nameof(reduction.Tokens.SummarizationTaskEndPrompt)} cannot be empty when {nameof(reduction.Tokens)} is used");

                    if (string.IsNullOrEmpty(reduction.Tokens.ResultPrefix))
                        throw new InvalidOperationException(
                            $"{nameof(reduction.Tokens.ResultPrefix)} cannot be empty when {nameof(reduction.Tokens)} is used");
                }
            }

            var duplicateNames = configuration.Parameters
                .GroupBy(p => p.Name)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();

            if (duplicateNames.Count > 0)
                throw new InvalidOperationException($"Duplicate parameter names found in agent configuration: {string.Join(", ", duplicateNames)}");

            var uniqueToolNames = new HashSet<string>(); // tool names are case-sensitive
            var scopeParams = configuration.Parameters.Select(x => x.Name).ToHashSet();
            foreach (var tool in configuration.Queries)
            {
                if (uniqueToolNames.Add(tool.Name) == false)
                    throw new InvalidOperationException($"Tool query name '{tool.Name}' is not unique. It is already defined in the agent configuration.");

                if (ToolNameChecker.IsMatch(tool.Name) == false)
                    throw new InvalidOperationException($"Query name '{tool.Name}' is invalid. It must match the pattern: {ToolNameChecker}");

                var q = QueryMetadata.ParseQuery(tool.Query, QueryType.Select);
                var queryParams = new HashSet<string>(q.Parameters.Select(x => x.Value));
                queryParams.ExceptWith(scopeParams);

                string paramsSchema = ChatCompletionClient.GetSchemaForTool(tool.ParametersSchema, tool.ParametersSampleObject);
                var schema = context.Sync.ReadForMemory(paramsSchema, "tool-schema");
                if (schema.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Required, out BlittableJsonReaderArray required))
                {
                    foreach (var arg in required)
                    {
                        string queryArg = arg.ToString();
                        if (scopeParams.Contains(queryArg))
                            throw new InvalidOperationException($"Parameter {queryArg} is defined on both the agent level and the query level for {tool.Name}");

                        queryParams.Remove(queryArg);
                    }
                }

                if (queryParams.Count > 0)
                    throw new InvalidOperationException(
                        $"Tool query '{tool.Name}' contains parameters that are not defined in the agent configuration: '{string.Join(", ", queryParams)}'");
            }

            foreach (var action in configuration.Actions)
            {
                if (uniqueToolNames.Add(action.Name) == false)
                    throw new InvalidOperationException($"Tool action name '{action.Name}' is not unique. It is already defined in the agent configuration.");

                if (ToolNameChecker.IsMatch(action.Name) == false)
                    throw new InvalidOperationException($"Action name '{action.Name}' is invalid. It must match the pattern: {ToolNameChecker}");
            }

            foreach (var tool in configuration.SubAgents)
            {
                if (tool.Identifier == configuration.Identifier)
                    throw new InvalidOperationException($"Agent '{tool.Identifier}' cannot be assigned as its own sub-agent. Use a different identifier for sub-agents.");

                if (uniqueToolNames.Add(tool.Identifier) == false)
                    throw new InvalidOperationException($"Sub-agent identifier '{tool.Identifier}' is already in use. Sub-agent identifiers must be unique.");

                if (ToolNameChecker.IsMatch(tool.Identifier) == false)
                    throw new InvalidOperationException(
                        $"Sub-agent identifier '{tool.Identifier}' is invalid. Use only letters, digits, '_' or '-' (pattern: {ToolNameChecker}).");
            }
        }
    }
}
