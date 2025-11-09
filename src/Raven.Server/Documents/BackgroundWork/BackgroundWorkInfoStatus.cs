namespace Raven.Server.Documents.BackgroundWork;

public enum BackgroundWorkInfoStatus
{
    Process,
    Skip,
    Delete,
    Conflict
}
