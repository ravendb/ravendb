namespace Raven.Database.Storage
{
	public interface IGeneralStorageActions
	{
		long GetNextIdentityValue(string name);
	}
}
