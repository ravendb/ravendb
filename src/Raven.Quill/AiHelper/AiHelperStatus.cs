namespace Raven.Quill.AiHelper;

public enum AiHelperStatus
{
    Success,
    InvalidCredentials,
    InvalidData,
    ConsentRequired,
    OutOfTokens,
    InternalError,
}
public static class AiHelperStatusExtensions
{
    public static bool ServiceAnswered(this AiHelperStatus status) =>
        status is AiHelperStatus.Success or AiHelperStatus.ConsentRequired or AiHelperStatus.InvalidCredentials;
}
