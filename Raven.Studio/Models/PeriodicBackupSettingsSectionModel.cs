using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;

namespace Raven.Studio.Models
{
	public class PeriodicBackupSettingsSectionModel : SettingsSectionModel
	{
		public PeriodicBackupSettingsSectionModel()
		{
			SectionName = "Periodic Backup";
		}

		public PeriodicBackupSetup PeriodicBackupSetup { get; set; }
		public string AwsAccessKey { get; set; }
		public string AwsSecretKey { get; set; }
        //TODO: remove original
        //TODO: add selection betweeb S3 and Glecuir
		public string OriginalAwsSecretKey { get; set; }
		public bool HasDocument { get; set; }

		public override void LoadFor(DatabaseDocument document)
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore
				.OpenAsyncSession(ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name);

			if (document.Settings.ContainsKey("Raven/AWSAccessKey") && document.SecuredSettings.ContainsKey("Raven/AWSSecretKey"))
			{
				AwsAccessKey = document.Settings["Raven/AWSAccessKey"];
				AwsSecretKey = document.SecuredSettings["Raven/AWSSecretKey"];
				OriginalAwsSecretKey = AwsSecretKey;
			}

			session.LoadAsync<PeriodicBackupSetup>(PeriodicBackupSetup.RavenDocumentKey).ContinueWith(task =>
			{
				PeriodicBackupSetup = task.Result;
				if (PeriodicBackupSetup == null)
					return;
				HasDocument = true;
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