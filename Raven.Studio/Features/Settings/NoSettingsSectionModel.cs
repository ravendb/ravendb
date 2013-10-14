using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
	public class NoSettingsSectionModel : SettingsSectionModel
	{
		public NoSettingsSectionModel()
		{
			SectionName = "No Settings available";
		}

		public override void MarkAsSaved()
		{
			HasUnsavedChanges = false;
		}
	}
}
