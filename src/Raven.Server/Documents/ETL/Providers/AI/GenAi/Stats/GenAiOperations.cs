namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;

public sealed class GenAiOperations
{
    public const string LoadToModel = "GenAI/LoadToModel";

    /// <summary>
    /// A sub-scope of <see cref="LoadToModel"/>. Covering the scope of loading the <see cref="AiAttachmentSource.Deferred"/> attachments.
    /// </summary>
    public const string LoadToModelRemoteAttachments = "GenAI/LoadToModel/RemoteAttachments";

    public const string ApplyUpdateScript = "GenAI/UpdatePhase";
}
