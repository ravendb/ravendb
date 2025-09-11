using System;
using System.Collections.Generic;
using Raven.Client.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class RequestBody
{
    public BlittableJsonReaderObject Parameters { get; set; }
    public string UserPrompt { get; set; }
    public BlittableJsonReaderArray ActionResponses { get; set; }
    public AiConversationCreationOptions CreationOptions { get; set; }

    public List<AiAttachment> Attachments { get; set; }

    public void ValidateForStart()
    {
        if (string.IsNullOrEmpty(UserPrompt))
            throw new ArgumentException("User prompt is missing.");

        if (Parameters == null)
            throw new ArgumentException(nameof(Parameters));
    }

    public void ValidateForResume()
    {
        if (string.IsNullOrEmpty(UserPrompt))
            throw new ArgumentException("User prompt is missing.");

        if (ActionResponses == null)
            throw new ArgumentException(nameof(ActionResponses));
    }
}
