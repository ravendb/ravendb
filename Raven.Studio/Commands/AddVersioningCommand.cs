using Raven.Bundles.Versioning.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class AddVersioningCommand: Command
	{
		private BaseBundlesModel bundlesModel;

		public AddVersioningCommand(BaseBundlesModel bundlesModel)
		{
			this.bundlesModel = bundlesModel;
		}

		public override void Execute(object parameter)
		{
			bundlesModel.VersioningConfigurations.Add(new VersioningConfiguration());
		}
	}
}
