using System;
using System.Collections.Generic;
using Raven.Client.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class RequestBody
{
    public BlittableJsonReaderObject Parameters { get; set; }
    public object UserPrompt { private get; set; }
    public BlittableJsonReaderArray ActionResponses { get; set; }
    
    public BlittableJsonReaderArray ArtificialActions { get; set; }
    public AiConversationCreationOptions CreationOptions { get; set; }

    public MergedBatchCommand AttachmentCommands { get; set; }

    public List<AiAttachment> Attachments { get; set; }

    public object Content
    {
        get
        {
            if (UserPrompt is BlittableJsonReaderArray array)
            {
                return array;
            }

            var promptText =  UserPrompt?.ToString();

            if (string.IsNullOrEmpty(promptText) && Attachments != null && Attachments.Count > 0)
                return string.Empty;

            return promptText;
        }
    }

    public void ValidateForStart()
    {
        if (HasUserPrompt(UserPrompt) == false)
            throw new ArgumentException("User prompt is missing.");

        if (Parameters == null)
            throw new ArgumentException(nameof(Parameters));
    }

    public void ValidateForResume()
    {
        if (HasUserPrompt(UserPrompt) == false)
            throw new ArgumentException("User prompt is missing.");

        if (ActionResponses == null)
            throw new ArgumentException(nameof(ActionResponses));
    }

    public static bool HasUserPrompt(object content)
    {
        if (content == null)
            return false;

        switch (content)
        {
            case string promptAsString:
                return string.IsNullOrEmpty(promptAsString) == false;

            case BlittableJsonReaderArray arr:
                if (arr.Length == 0)
                    return false;

                foreach (var part in arr)
                {
                    if (part is not BlittableJsonReaderObject obj)
                    {
                        return false;
                    }

                    if (obj.TryGet(AiMessagePromptFields.Type, out string typeValue) == false)
                        return false;

                    if (typeValue == AiMessagePromptTypes.Text)
                    {
                        if (obj.TryGet(AiMessagePromptFields.Text, out string textValue) == false || string.IsNullOrEmpty(textValue))
                            return false;

                        continue;
                    }

                    if (typeValue == AiMessagePromptFields.Image)
                    {
                        continue;
                    }

                    return false;
                }
                return true;
            default:
                return false;
        }
    }
}
