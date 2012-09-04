using Raven.Bundles.Versioning.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteVersioningCommand : Command
	{
		private readonly BaseSettingsModel settingsModel;

		public DeleteVersioningCommand(BaseSettingsModel settingsModel)
		{
			this.settingsModel = settingsModel;
		}

		public override void Execute(object parameter)
		{
			var versioningConfiguration = parameter as VersioningConfiguration;
			if (versioningConfiguration == null)
				return;

			settingsModel.VersioningConfigurations.Remove(versioningConfiguration);
			var test = settingsModel.HasDefaultVersioning;
			settingsModel.SeletedVersioning = null;
		}
	}
}