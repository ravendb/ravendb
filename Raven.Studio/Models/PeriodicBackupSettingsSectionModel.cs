using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
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

		public PeriodicBackupSetup PeriodicBackupSetup { get; set; }
		public string AwsAccessKey { get; set; }
		public string AwsSecretKey { get; set; }
		public Observable<int> SelectedOption { get; set; }
		public bool HasDocument { get; set; }

		public override void LoadFor(DatabaseDocument document)
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore
				.OpenAsyncSession(ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name);

			if (document.Settings.ContainsKey("Raven/AWSAccessKey") && document.SecuredSettings.ContainsKey("Raven/AWSSecretKey"))
			{
				AwsAccessKey = document.Settings["Raven/AWSAccessKey"];
				AwsSecretKey = document.SecuredSettings["Raven/AWSSecretKey"];
			}

			session.LoadAsync<PeriodicBackupSetup>(PeriodicBackupSetup.RavenDocumentKey).ContinueWith(task =>
			{
				PeriodicBackupSetup = task.Result;
				if (PeriodicBackupSetup == null)
					return;
				HasDocument = true;
				if (string.IsNullOrWhiteSpace(PeriodicBackupSetup.LocalFolderName) == false)
					SelectedOption.Value = 0;
				else if (string.IsNullOrWhiteSpace(PeriodicBackupSetup.GlacierVaultName) == false)
					SelectedOption.Value = 1;
				else if (string.IsNullOrWhiteSpace(PeriodicBackupSetup.S3BucketName) == false)
					SelectedOption.Value = 2;
				OnPropertyChanged(() => HasDocument);
				OnPropertyChanged(() => PeriodicBackupSetup);
			});
		}

		public ICommand EnablePeriodicBackup{get{return new ActionCommand(() =>
		{
			PeriodicBackupSetup = new PeriodicBackupSetup();
			HasDocument = true;
			OnPropertyChanged(() => HasDocument);
			OnPropertyChanged(() => PeriodicBackupSetup);
		});}}
	}
}