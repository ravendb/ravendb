using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Sparrow.Json.Parsing;

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

            if (EmbeddingsGenerationConfiguration.ValidateIdentifier(Configuration.Identifier, out var errors) == false)
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
    }
}
