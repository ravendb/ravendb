namespace Raven.Server.Documents.AI.AiAssistant.Responses;

public enum AiResponseStatus
{
    Success,
    InvalidCredentials,
    InvalidData,
    ConsentRequired,
    OutOfTokens
}
