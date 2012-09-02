namespace Raven.Database.Storage.RAM
{
	public class RamGeneralStorageActions : IGeneralStorageActions
	{
		private readonly RamState state;

		public RamGeneralStorageActions(RamState state)
		{
			this.state = state;
		}

		public long GetNextIdentityValue(string name)
		{
			return state.Identities.GetOrAdd(name).Value += 1;
		}
	}
}