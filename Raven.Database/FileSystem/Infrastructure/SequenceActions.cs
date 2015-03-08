using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Storage;

namespace Raven.Database.FileSystem.Infrastructure
{
	public class SequenceActions
	{
		private const string SequencesKeyPrefix = "Raven/Sequences/";
		private readonly ITransactionalStorage storage;

		public SequenceActions(ITransactionalStorage storage)
		{
			this.storage = storage;
		}

		public long GetNextValue(string name)
		{
			long result = 1;
			storage.Batch(
				accessor =>
				{
					var sequenceName = SequenceName(name);
					accessor.TryGetConfigurationValue(sequenceName, out result);
					result++;
					accessor.SetConfigurationValue(sequenceName, result);
				});
			return result;
		}

		private static string SequenceName(string name)
		{
			return SequencesKeyPrefix + name;
		}
	}
}
