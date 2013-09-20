using System.Windows.Controls;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
	public class PeriodicBackupSettingsSectionModel : SettingsSectionModel
	{
		public Observable<bool> ShowPeriodicBackup { get; set; }
		public PeriodicBackupSettingsSectionModel()
		{
			SectionName = "Periodic Backup";
			SelectedOption = new Observable<int>();
			ShowPeriodicBackup = new Observable<bool>();

			var req = ApplicationModel.DatabaseCommands.ForSystemDatabase().CreateRequest("/license/status".NoCache(), "GET");

			req.ReadResponseJsonAsync().ContinueOnSuccessInTheUIThread(doc =>
			{
				var licensingStatus = ((RavenJObject)doc).Deserialize<LicensingStatus>(new DocumentConvention());
				if (licensingStatus != null && licensingStatus.Attributes != null)
				{
					string active;
					if (licensingStatus.Attributes.TryGetValue("PeriodicBackup", out active) == false)
						ShowPeriodicBackup.Value = true;
					else
					{
						bool result;
						bool.TryParse(active, out result);
						ShowPeriodicBackup.Value = result;
					}

					OnPropertyChanged(() => ShowPeriodicBackup);
				}
			});
		}

		public override void CheckForChanges()
		{
			if (HasUnsavedChanges)
				return;

			if (PeriodicBackupSettings.Equals(OriginalPeriodicBackupSettings) == false)
			{
				HasUnsavedChanges = true;
				return;
			}
			if (PeriodicBackupSetup == null)
			{
				if(OriginalPeriodicBackupSetup == null)
					return;
				HasUnsavedChanges = true;
				return;
			}

			if (PeriodicBackupSetup.Equals(OriginalPeriodicBackupSetup) == false)
				HasUnsavedChanges = true;
		}

		public override void MarkAsSaved()
		{
			HasUnsavedChanges = false;
			OriginalPeriodicBackupSettings = PeriodicBackupSettings;
			OriginalPeriodicBackupSetup = PeriodicBackupSetup;
		}

		public PeriodicBackupSetup PeriodicBackupSetup { get; set; }
		public PeriodicBackupSetup OriginalPeriodicBackupSetup { get; set; }
		public PeriodicBackupSettings PeriodicBackupSettings { get; set; }
		public PeriodicBackupSettings OriginalPeriodicBackupSettings { get; set; }
		public Observable<int> SelectedOption { get; set; }
		public bool HasDocument { get; set; }

		public override void LoadFor(DatabaseDocument document)
		{
			PeriodicBackupSettings = new PeriodicBackupSettings();
			var session = ApplicationModel.Current.Server.Value.DocumentStore
				.OpenAsyncSession(ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name);

			if (document.Settings.ContainsKey("Raven/AWSAccessKey") && document.SecuredSettings.ContainsKey("Raven/AWSSecretKey"))
			{
				PeriodicBackupSettings.AwsAccessKey = document.Settings["Raven/AWSAccessKey"];
				PeriodicBackupSettings.AwsSecretKey = document.SecuredSettings["Raven/AWSSecretKey"];
			}

		    if (document.Settings.ContainsKey("Raven/AzureStorageAccount") && document.SecuredSettings.ContainsKey("Raven/AzureStorageKey"))
		    {
		        PeriodicBackupSettings.AzureStorageAccount = document.Settings["Raven/AzureStorageAccount"];
		        PeriodicBackupSettings.AzureStorageKey = document.SecuredSettings["Raven/AzureStorageKey"];
		    }

			OriginalPeriodicBackupSettings = new PeriodicBackupSettings
			{
				AwsAccessKey = PeriodicBackupSettings.AwsAccessKey,
				AwsSecretKey = PeriodicBackupSettings.AwsSecretKey,
				AzureStorageAccount = PeriodicBackupSettings.AzureStorageAccount,
				AzureStorageKey = PeriodicBackupSettings.AzureStorageKey
			};

		    session.LoadAsync<PeriodicBackupSetup>(PeriodicBackupSetup.RavenDocumentKey).ContinueWith(task =>
			{
				PeriodicBackupSetup = task.Result;
				
				if (PeriodicBackupSetup == null)
					return;

				OriginalPeriodicBackupSetup = new PeriodicBackupSetup
				{
					AwsRegionEndpoint = PeriodicBackupSetup.AwsRegionEndpoint,
					AzureStorageContainer = PeriodicBackupSetup.AzureStorageContainer,
					GlacierVaultName = PeriodicBackupSetup.GlacierVaultName,
					IntervalMilliseconds = PeriodicBackupSetup.IntervalMilliseconds,
					LocalFolderName = PeriodicBackupSetup.LocalFolderName,
					S3BucketName = PeriodicBackupSetup.S3BucketName
				};

				HasDocument = true;
				if (string.IsNullOrWhiteSpace(PeriodicBackupSetup.LocalFolderName) == false)
					SelectedOption.Value = 0;
				else if (string.IsNullOrWhiteSpace(PeriodicBackupSetup.GlacierVaultName) == false)
					SelectedOption.Value = 1;
				else if (string.IsNullOrWhiteSpace(PeriodicBackupSetup.S3BucketName) == false)
					SelectedOption.Value = 2;
                else if (string.IsNullOrWhiteSpace(PeriodicBackupSetup.AzureStorageContainer) == false)
                    SelectedOption.Value = 3;
				OnPropertyChanged(() => HasDocument);
				OnPropertyChanged(() => PeriodicBackupSetup);
			});
		}

		public ICommand EnablePeriodicBackup
		{
			get
			{
				return new ActionCommand(() =>
				{
					PeriodicBackupSetup = new PeriodicBackupSetup();
					HasDocument = true;
					OnPropertyChanged(() => HasDocument);
					OnPropertyChanged(() => PeriodicBackupSetup);
				});
			}
		}

		public ComboBoxItem SelectedAwsRegionEndpoint
		{
			get { return new ComboBoxItem(); }
			set { PeriodicBackupSetup.AwsRegionEndpoint = value.Tag.ToString(); }
		}
	}

	public class PeriodicBackupSettings
	{
		public string AwsAccessKey { get; set; }
		public string AwsSecretKey { get; set; }
		public string AzureStorageAccount { get; set; }
		public string AzureStorageKey { get; set; }

		protected bool Equals(PeriodicBackupSettings other)
		{
			return string.Equals(AwsAccessKey, other.AwsAccessKey) && string.Equals(AwsSecretKey, other.AwsSecretKey) &&
			       string.Equals(AzureStorageAccount, other.AzureStorageAccount) &&
			       string.Equals(AzureStorageKey, other.AzureStorageKey);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((PeriodicBackupSettings) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (AwsAccessKey != null ? AwsAccessKey.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (AwsSecretKey != null ? AwsSecretKey.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (AzureStorageAccount != null ? AzureStorageAccount.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (AzureStorageKey != null ? AzureStorageKey.GetHashCode() : 0);
				return hashCode;
			}
		}
	}
}