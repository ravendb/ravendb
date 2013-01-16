using System;
using System.Collections.ObjectModel;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.SqlReplication;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class SqlReplicationSettingsSectionModel : SettingsSectionModel
	{
		private ICommand addReplicationCommand;
        private ICommand deleteReplicationCommand;

        public SqlReplicationSettingsSectionModel()
        {
            SqlReplicationConfigs = new ObservableCollection<SqlReplicationConfig>();
            SectionName = "Sql Replication";
        }

        public ICommand DeleteReplication
        {
            get { return deleteReplicationCommand ?? (deleteReplicationCommand = new ActionCommand(HandleDeleteReplication)); }
        }

        public ICommand AddReplication
        {
            get
            {
                return addReplicationCommand ??
                       (addReplicationCommand =
                        new ActionCommand(() => SqlReplicationConfigs.Add(new SqlReplicationConfig())));
            }
        }

		public SqlReplicationConfig SelectedReplication { get; set; }

		public ObservableCollection<SqlReplicationConfig> SqlReplicationConfigs { get; set; }

        private void HandleDeleteReplication(object parameter)
        {
			var destination = parameter as SqlReplicationConfig;
            if (destination == null)
                return;
            SqlReplicationConfigs.Remove(destination);
            SelectedReplication = null;
        }

        public override void LoadFor(DatabaseDocument database)
        {
            ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(database.Id)
				.Advanced.LoadStartingWithAsync<SqlReplicationConfig>("Raven/SqlReplication/Configuration/")
                .ContinueOnSuccessInTheUIThread(documents =>
                {
                    if (documents == null)
                        return;

                    SqlReplicationConfigs = new ObservableCollection<SqlReplicationConfig>(documents);
                });
        }

		public void UpdateIds()
		{
			foreach (var sqlReplicationConfig in SqlReplicationConfigs)
			{
				sqlReplicationConfig.Id = "Raven/SqlReplication/Configuration/" + sqlReplicationConfig.Name;
			}
		}
	}
}
