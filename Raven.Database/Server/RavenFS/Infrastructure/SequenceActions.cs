using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;

namespace Raven.Database.Server.RavenFS.Infrastructure
{
	public class SequenceActions
	{
		private const string SequencesKeyPrefix = "Raven/Sequences/";
		private readonly TransactionalStorage storage;

		public SequenceActions(TransactionalStorage storage)
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
