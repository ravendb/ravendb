namespace Raven.Database.Data
{
    public interface ICommandData
    {
		string Key { get; }
		string Method { get; }
        TransactionInformation TransactionInformation { get; }
    	void Execute(DocumentDatabase database);
    }
}
