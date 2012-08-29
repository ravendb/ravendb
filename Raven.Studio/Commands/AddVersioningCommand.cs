using Raven.Bundles.Versioning.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class AddVersioningCommand: Command
	{
		private BaseSettingsModel settingsModel;

		public AddVersioningCommand(BaseSettingsModel settingsModel)
		{
			this.settingsModel = settingsModel;
		}

		public override void Execute(object parameter)
		{
			settingsModel.VersioningConfigurations.Add(new VersioningConfiguration());
		}
	}
}
