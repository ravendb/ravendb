namespace Raven.Server.Documents.BackgroundWork;

public enum DocumentExpirationInfoStatus
{
    Process,
    Skip,
    Delete,
    Conflict
}
