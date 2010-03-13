namespace Raven.Database
{
    public enum PatchResult
    {
        DocumentDoesNotExists,
        WriteConflict,
        Patched
    }
}