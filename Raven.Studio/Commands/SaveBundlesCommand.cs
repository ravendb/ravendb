using System;
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

            var quotaSettings = settingsModel.GetSection<QuotaSettingsSectionModel>();
            if (quotaSettings != null)
			{
				settingsModel.DatabaseDocument.Settings[Constants.SizeHardLimitInKB] =
                    (quotaSettings.MaxSize * 1024).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.SizeSoftLimitInKB] =
                    (quotaSettings.WarnSize * 1024).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.DocsHardLimit] =
                    (quotaSettings.MaxDocs).ToString(CultureInfo.InvariantCulture);
				settingsModel.DatabaseDocument.Settings[Constants.DocsSoftLimit] =
                    (quotaSettings.WarnDocs).ToString(CultureInfo.InvariantCulture);
				if (settingsModel.DatabaseDocument.Id == null)
					settingsModel.DatabaseDocument.Id = databaseName;
				DatabaseCommands.CreateDatabaseAsync(settingsModel.DatabaseDocument);
			}

		    var replicationSettings = settingsModel.GetSection<ReplicationSettingsSectionModel>();
            if (replicationSettings != null)
			{
				session.LoadAsync<ReplicationDocument>("Raven/Replication/Destinations")
					.ContinueOnSuccessInTheUIThread(document =>
					{
                        if (document == null)
                        {
                            document = new ReplicationDocument();
                        }

						document.Destinations.Clear();
                        foreach (var destination in replicationSettings.ReplicationDestinations
							.Where(destination => !string.IsNullOrWhiteSpace(destination.Url) || !string.IsNullOrWhiteSpace(destination.ConnectionStringName)))
						{
							document.Destinations.Add(destination);
						}
						
						session.Store(document);
					});
			}

            var versioningSettings = settingsModel.GetSection<VersioningSettingsSectionModel>();
            if (versioningSettings != null)
			{
                var versionsToDelete = versioningSettings.OriginalVersioningConfigurations
					.Where(
						originalVersioningConfiguration =>
                        versioningSettings.VersioningConfigurations.Contains(originalVersioningConfiguration) == false)
					.ToList();
				foreach (var versioningConfiguration in versionsToDelete)
				{
					DatabaseCommands.DeleteDocumentAsync(versioningConfiguration.Id);
				}

                foreach (var versioningConfiguration in versioningSettings.VersioningConfigurations)
				{
					if (versioningConfiguration.Id.StartsWith("Raven/Versioning/",StringComparison.InvariantCultureIgnoreCase) == false)
						versioningConfiguration.Id = "Raven/Versioning/" + versioningConfiguration.Id;
					session.Store(versioningConfiguration);
				}
			}

			session.SaveChangesAsync()
				.ContinueOnSuccessInTheUIThread(() =>
				{
					ApplicationModel.Current.AddNotification(new Notification("Updated Settings for: " + databaseName));
				});
		}
	}
}
