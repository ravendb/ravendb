using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class PeriodicBackupSettingsSectionModel : SettingsSectionModel
	{
		public PeriodicBackupSettingsSectionModel()
		{
			SectionName = "Periodic Backup";
            IsS3Selected = new Observable<bool>();
		}

		public PeriodicBackupSetup PeriodicBackupSetup { get; set; }
		public string AwsAccessKey { get; set; }
		public string AwsSecretKey { get; set; }
        public Observable<bool> IsS3Selected { get; set; }
        //TODO: add selection betweeb S3 and Glecuir
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