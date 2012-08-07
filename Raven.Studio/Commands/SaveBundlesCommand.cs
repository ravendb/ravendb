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
		private readonly BundlesModel bundlesModel;

		public SaveBundlesCommand(BundlesModel bundlesModel)
		{
			this.bundlesModel = bundlesModel;
		}

		public override void Execute(object parameter)
		{
			var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName);

			if (bundlesModel.HasQuotas)
			{
				bundlesModel.DatabaseDocument.Settings[Constants.SizeHardLimitInKB] =
					(bundlesModel.MaxSize*1024).ToString(CultureInfo.InvariantCulture);
				bundlesModel.DatabaseDocument.Settings[Constants.SizeSoftLimitInKB] =
					(bundlesModel.WarnSize*1024).ToString(CultureInfo.InvariantCulture);
				bundlesModel.DatabaseDocument.Settings[Constants.DocsHardLimit] =
					(bundlesModel.MaxDocs).ToString(CultureInfo.InvariantCulture);
				bundlesModel.DatabaseDocument.Settings[Constants.DocsSoftLimit] =
					(bundlesModel.WarnDocs).ToString(CultureInfo.InvariantCulture);
				if (bundlesModel.DatabaseDocument.Id == null)
					bundlesModel.DatabaseDocument.Id = databaseName;
				DatabaseCommands.CreateDatabaseAsync(bundlesModel.DatabaseDocument);
			}

			if (bundlesModel.HasReplication)
			{
				session.LoadAsync<ReplicationDocument>("Raven/Replication/Destinations")
					.ContinueOnSuccessInTheUIThread(document =>
					{
						document.Destinations = bundlesModel.ReplicationDestinations.ToList();
						session.Store(document);
					});
			}

			if (bundlesModel.HasVersioning)
			{
				var versionsToDelete = bundlesModel.OriginalVersioningConfigurations
					.Where(
						originalVersioningConfiguration =>
						bundlesModel.VersioningConfigurations.Contains(originalVersioningConfiguration) == false)
					.ToList();
				foreach (var versioningConfiguration in versionsToDelete)
				{
					DatabaseCommands.DeleteDocumentAsync(versioningConfiguration.Id);
				}

				foreach (var versioningConfiguration in bundlesModel.VersioningConfigurations)
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
