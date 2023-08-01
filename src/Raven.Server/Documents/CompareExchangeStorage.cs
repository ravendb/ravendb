namespace Raven.Server.Documents;

public sealed class CompareExchangeStorage : AbstractCompareExchangeStorage
{
    public CompareExchangeStorage(DocumentDatabase database)
        : base(database.ServerStore)
    {
    }
}
