using Raven.Bundles.Versioning.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteVersioningCommand : Command
	{
		private readonly BaseBundlesModel bundlesModel;

		public DeleteVersioningCommand(BaseBundlesModel bundlesModel)
		{
			this.bundlesModel = bundlesModel;
		}

		public override void Execute(object parameter)
		{
			var versioningConfiguration = parameter as VersioningConfiguration;
			if (versioningConfiguration == null)
				return;

			bundlesModel.VersioningConfigurations.Remove(versioningConfiguration);
			var test = bundlesModel.HasDefaultVersioning;
			bundlesModel.SeletedVersioning = null;
		}
	}
}