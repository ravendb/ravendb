namespace Raven.Database.Storage.StorageActions
{
	public interface IGeneralStorageActions
	{
		long GetNextIdentityValue(string name);
	}
}