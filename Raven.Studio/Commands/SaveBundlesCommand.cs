using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Extensions;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Commands
{
	public class SaveBundlesCommand : Command
	{
		private readonly SettingsModel settingsModel;

		public SaveBundlesCommand(SettingsModel settingsModel)
		{
			this.settingsModel = settingsModel;
		}

		public override void Execute(object parameter)
		{
			var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName);

			if (settingsModel.HasQuotas)
			{
				settingsModel.DatabaseDocument.Settings[Constants.SizeHardLimitInKB] =
					(settingsModel.MaxSize*1024).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.SizeSoftLimitInKB] =
					(settingsModel.WarnSize*1024).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.DocsHardLimit] =
					(settingsModel.MaxDocs).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.DocsSoftLimit] =
					(settingsModel.WarnDocs).ToString(CultureInfo.InvariantCulture);
				if (settingsModel.DatabaseDocument.Id == null)
					settingsModel.DatabaseDocument.Id = databaseName;
				DatabaseCommands.CreateDatabaseAsync(settingsModel.DatabaseDocument);
			}

			if (settingsModel.HasReplication)
			{
				session.LoadAsync<ReplicationDocument>("Raven/Replication/Destinations")
					.ContinueOnSuccessInTheUIThread(document =>
					{
						document.Destinations.Clear();
						foreach (var destination in settingsModel.ReplicationDestinations
							.Where(destination => !string.IsNullOrWhiteSpace(destination.Url) || !string.IsNullOrWhiteSpace(destination.ConnectionStringName)))
						{
							document.Destinations.Add(destination);
						}
						
						session.Store(document);
					});
			}

			if (settingsModel.HasVersioning)
			{
				var versionsToDelete = settingsModel.OriginalVersioningConfigurations
					.Where(
						originalVersioningConfiguration =>
						settingsModel.VersioningConfigurations.Contains(originalVersioningConfiguration) == false)
					.ToList();
				foreach (var versioningConfiguration in versionsToDelete)
				{
					DatabaseCommands.DeleteDocumentAsync(versioningConfiguration.Id);
				}

				foreach (var versioningConfiguration in settingsModel.VersioningConfigurations)
				{
					session.Store(versioningConfiguration);
				}
			}

			session.SaveChangesAsync()
				.ContinueOnSuccessInTheUIThread(() =>
				{
					ApplicationModel.Current.AddNotification(new Notification("Updated Bundles for: " + databaseName));
					UrlUtil.Navigate("/databases");
				});
		}
	}
}
