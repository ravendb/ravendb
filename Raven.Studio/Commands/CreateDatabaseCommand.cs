using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using System.Windows.Controls;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Versioning.Data;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Studio.Controls;
using Raven.Studio.Features.Bundles;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Commands
{
	public class CreateDatabaseCommand : Command
	{
		public override void Execute(object parameter)
		{
			new NewDatabase().ShowAsync()
				.ContinueOnSuccessInTheUIThread(newDatabase =>
				{
					var databaseName = newDatabase.DbName.Text;

					if (Path.GetInvalidPathChars().Any(databaseName.Contains))
						throw new ArgumentException("Cannot create a database with invalid path characters: " + databaseName);
					if (ApplicationModel.Current.Server.Value.Databases.Count(s => s == databaseName) != 0)
						throw new ArgumentException("A database with the name " + databaseName + " already exists");

					AssertValidName(databaseName);

					new BundlesSelect().ShowAsync()
						.ContinueOnSuccessInTheUIThread(bundles =>
						{
							var bundlesModel = new CreateBundlesModel();
							var bundlesSettings = new List<ChildWindow>();
							if (bundles.Encryption.IsChecked == true)
								bundlesSettings.Add(new EncryptionSettings());
							if (bundles.Quotas.IsChecked == true || bundles.Replication.IsChecked == true || bundles.Versioning.IsChecked == true)
							{
								bundlesModel = new CreateBundlesModel()
									{
										HasQuotas = bundles.Quotas.IsChecked == true,
										HasReplication = bundles.Replication.IsChecked == true,
										HasVersioning = bundles.Versioning.IsChecked == true
									};
								if (bundlesModel.HasQuotas)
								{
									bundlesModel.Bundles.Add("Quotas");
									bundlesModel.SelectedBundle.Value = "Quotas";
								}
								if (bundlesModel.HasReplication)
								{ 
									bundlesModel.Bundles.Add("Replication");
									if(bundlesModel.SelectedBundle.Value == null)
										bundlesModel.SelectedBundle.Value = "Replication";
								}
								if (bundlesModel.HasVersioning)
								{
									bundlesModel.Bundles.Add("Versioning");
									if (bundlesModel.SelectedBundle.Value == null)
										bundlesModel.SelectedBundle.Value = "Versioning";
								}

								var bundleView = new BundlesView()
								{
									DataContext = bundlesModel
								};

								bundlesSettings.Add(new ChildWindow()
								{
									Title = "Setup bundles",
									Content = bundleView
								});
							}

							new Wizard(bundlesSettings).StartAsync()
								.ContinueOnSuccessInTheUIThread(bundlesData =>
								{
									ApplicationModel.Current.AddNotification(new Notification("Creating database: " + databaseName));
									var settings = UpdateSettings(newDatabase, bundles, bundlesModel);
									var securedSettings = UpdateSecuredSettings(bundlesData);

									var databaseDocuemnt = new DatabaseDocument
									{
										Id = newDatabase.DbName.Text,
										Settings = settings,
										SecuredSettings = securedSettings
									};

									string encryptionKey = null;
									var encryotionSettings = bundlesData.FirstOrDefault(window => window is EncryptionSettings) as EncryptionSettings;
									if (encryotionSettings != null)
										encryptionKey = encryotionSettings.EncryptionKey.Text;

									DatabaseCommands.CreateDatabaseAsync(databaseDocuemnt).ContinueOnSuccess(
										() => DatabaseCommands.ForDatabase(databaseName).EnsureSilverlightStartUpAsync())
										.ContinueOnSuccessInTheUIThread(() =>
										{
											var model = parameter as DatabasesListModel;
											if (model != null)
												model.ForceTimerTicked();
											ApplicationModel.Current.AddNotification(
												new Notification("Database " + databaseName + " created"));

											HendleBundleAfterCreation(bundlesModel, databaseName, encryptionKey);

											ExecuteCommand(new ChangeDatabaseCommand(), databaseName);
										})
										.Catch();
								});
						});
				})
				.Catch();
		}

		private Dictionary<string, string> UpdateSecuredSettings(IEnumerable<ChildWindow> bundlesData)
		{
			var settings = new Dictionary<string, string>();


			var encryptionData = bundlesData.FirstOrDefault(window => window is EncryptionSettings) as EncryptionSettings;
			if (encryptionData != null)
			{
				settings[Constants.EncryptionKeySetting] = encryptionData.EncryptionKey.Text;
			}

			return settings;
		}

		private void HendleBundleAfterCreation(CreateBundlesModel bundlesModel, string databaseName, string encryptionKey)
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName);
			if (bundlesModel.HasVersioning)
				StoreVersioningData(bundlesModel.VersioningConfigurations, session);

			if (bundlesModel.HasReplication)
			{
				var replicationDocument = new ReplicationDocument
				{
					Destinations = new List<ReplicationDestination>(bundlesModel.ReplicationDestinations)
				};
				session.Store(replicationDocument);
			}

			session.SaveChangesAsync();

			if (!string.IsNullOrEmpty(encryptionKey))
				new ShowEncryptionMessage(encryptionKey).Show();

		}

		private void StoreVersioningData(IEnumerable<VersioningConfiguration> versioningData, IAsyncDocumentSession session)
		{
			foreach (var data in versioningData)
			{
				session.Store(data);
			}
		}

		private static Dictionary<string, string> UpdateSettings(NewDatabase newDatabase, BundlesSelect bundles, CreateBundlesModel bundlesData)
		{
			var settings = new Dictionary<string, string>
			{
				{
					Constants.RavenDataDir, newDatabase.ShowAdvanced.IsChecked == true
					                 	? newDatabase.DbPath.Text
					                 	: Path.Combine("~", Path.Combine("Databases", newDatabase.DbName.Text))
					},
				{Constants.ActiveBundles, string.Join(";", bundles.Bundles)}
			};

			if (!string.IsNullOrWhiteSpace(newDatabase.LogsPath.Text))
				settings.Add(Constants.RavenLogsPath, newDatabase.LogsPath.Text);
			if (!string.IsNullOrWhiteSpace(newDatabase.IndexPath.Text))
				settings.Add(Constants.RavenIndexPath, newDatabase.IndexPath.Text);

			if (bundlesData.HasQuotas)
			{
				settings[Constants.DocsHardLimit] = (bundlesData.MaxDocs).ToString(CultureInfo.InvariantCulture);
				settings[Constants.DocsSoftLimit] = (bundlesData.WarnDocs).ToString(CultureInfo.InvariantCulture);
				settings[Constants.SizeHardLimitInKB] = (bundlesData.MaxSize * 1024).ToString(CultureInfo.InvariantCulture);
				settings[Constants.SizeSoftLimitInKB] = (bundlesData.WarnSize * 1024).ToString(CultureInfo.InvariantCulture);
			}

			return settings;
		}
		
		private static readonly string validDbNameChars = @"([A-Za-z0-9_\-\.]+)";

		public static void AssertValidName(string name)
		{
			if (name == null) throw new ArgumentNullException("name");
			var result = Regex.Matches(name, validDbNameChars);
			if (result.Count == 0 || result[0].Value != name)
			{
				throw new InvalidOperationException("Database name can only contain only A-Z, a-z, \"_\", \".\" or \"-\" but was: " + name);
			}
		}
	}
}