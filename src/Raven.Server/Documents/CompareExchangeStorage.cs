namespace Raven.Server.Documents;

public class CompareExchangeStorage : AbstractCompareExchangeStorage
{
    public CompareExchangeStorage(DocumentDatabase database)
        : base(database.ServerStore)
    {
    }
}
