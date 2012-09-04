using Raven.Abstractions.Replication;
using Raven.Bundles.Versioning.Data;

namespace Raven.Studio.Models
{
	public sealed class CreateSettingsModel : BaseSettingsModel
	{
		public CreateSettingsModel()
		{
			MaxSize = 50;
			WarnSize = 45;
			MaxDocs = 10000;
			WarnDocs = 8000;
			Creation = true;

			ReplicationDestinations.Add(new ReplicationDestination());
			VersioningConfigurations.Add(new VersioningConfiguration()
			{
				Exclude = false,
				Id = "Raven/Versioning/DefaultConfiguration",
				MaxRevisions = 5
			});
		}
	}
}