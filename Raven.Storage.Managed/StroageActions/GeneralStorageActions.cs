using Raven.Database.Storage.StorageActions;

namespace Raven.Storage.Managed.StroageActions
{
	public class GeneralStorageActions : AbstractStorageActions, IGeneralStorageActions
	{
		public long GetNextIdentityValue(string name)
		{
			var currentValue = Mutator.Identity.FindValue(name) ?? 1;
			Mutator.Identity.Add(name, currentValue + 1);
			return currentValue;
		}
	}
}