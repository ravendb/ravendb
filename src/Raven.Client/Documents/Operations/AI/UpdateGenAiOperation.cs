using System.Net.Http;
using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Updates an existing GenAI task definition.
/// Use this operation to modify the configuration of an existing task and optionally reset its transformation.
/// </summary>
public class UpdateGenAiOperation(long taskId, GenAiConfiguration configuration, StartingPointChangeVector startingPoint = null, bool reset = false) : IMaintenanceOperation<UpdateEtlOperationResult>
{
    /// <summary>
    /// Creates the command that will be executed by the maintenance executor.
    /// </summary>
    public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        List<string> transformationsToReset = null;

        if (reset)
            transformationsToReset = [configuration.TransformationName];
        
        return new UpdateGenAiCommand(conventions, taskId, configuration, startingPoint, transformationsToReset);
    }

    /// <summary>
    /// Server command responsible for sending the updated GenAI configuration to the database.
    /// </summary>
    internal sealed class UpdateGenAiCommand : UpdateEtlOperation<AiConnectionString>.UpdateEtlCommand
    {
        private readonly StartingPointChangeVector _startingPoint;

        /// <summary>
        /// Initializes the command.
        /// </summary>
        /// <param name="conventions">Serialization conventions used to generate the request payload.</param>
        /// <param name="taskId">The server-side identifier of the task to update.</param>
        /// <param name="configuration">The updated GenAI ETL configuration.</param>
        /// <param name="startingPoint">
        /// The change vector from which the ETL should start.
        /// When <see langword="null"/>, defaults to <see cref="StartingPointChangeVector.DoNotChange"/>.
        /// </param>
        /// <param name="transformationsToReset">
        /// Optional list of transformations to reset (effectively restarting progress for those transformations).
        /// </param>
        public UpdateGenAiCommand(DocumentConventions conventions, long taskId, GenAiConfiguration configuration, StartingPointChangeVector startingPoint, List<string> transformationsToReset) : base(conventions, taskId, configuration, transformationsToReset)
        {
            _startingPoint = startingPoint ?? StartingPointChangeVector.DoNotChange;
        }

        /// <inheritdoc />
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            url += $"&changeVector={Uri.EscapeDataString(_startingPoint.Value)}";

            return request;
        }
    }
}
