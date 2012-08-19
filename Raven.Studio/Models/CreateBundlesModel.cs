using Raven.Abstractions.Replication;

namespace Raven.Studio.Models
{
	public sealed class CreateBundlesModel : BaseBundlesModel
	{
		public CreateBundlesModel()
		{
			MaxSize = 50;
			WarnSize = 45;
			MaxDocs = 10000;
			WarnDocs = 8000;
			Creation = true;

			ReplicationDestinations.Add(new ReplicationDestination());
		}
	}
}
