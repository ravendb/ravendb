using Raven.Bundles.Versioning.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class AddDefaultVersioningCommand : Command
	{
		private BaseBundlesModel baseBundlesModel;

		public AddDefaultVersioningCommand(BaseBundlesModel baseBundlesModel)
		{
			this.baseBundlesModel = baseBundlesModel;
		}

		public override void Execute(object parameter)
		{
			baseBundlesModel.VersioningConfigurations.Add(new VersioningConfiguration()
			{
				Exclude = false,
				Id = "Raven/Versioning/DefaultConfiguration",
				MaxRevisions = 5
			});
		}
	}
}
