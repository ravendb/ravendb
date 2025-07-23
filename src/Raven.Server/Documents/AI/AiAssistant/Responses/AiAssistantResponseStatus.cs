namespace Raven.Server.Documents.AI.AiAssistant.Responses;

public enum AiAssistantResponseStatus
{
    Success,
    InvalidCredentials,
    InvalidData,
    ConsentRequired,
    OutOfTokens
}
