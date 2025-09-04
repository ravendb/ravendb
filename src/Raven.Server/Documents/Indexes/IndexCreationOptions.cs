namespace Raven.Server.Documents.Indexes
{
    public enum IndexCreationOptions
    {
        Noop,
        Update,
        UpdateWithoutUpdatingCompiledIndex,
        Create
    }
}