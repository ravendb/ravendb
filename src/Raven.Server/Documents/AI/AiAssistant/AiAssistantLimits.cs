using System;

namespace Raven.Server.Documents.AI.AiAssistant;

public class AiAssistantLimits
{
    public float Quota { get; set; }
    public DateTime ResetTime { get; set; }
}
