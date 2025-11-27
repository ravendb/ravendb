namespace Raven.Server.Documents.BackgroundWork;

public enum BackgroundWorkInfoStatus
{
    Process,
    Delete,
    Conflict,
    Retry,
    Skip
}
