using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using Raven.Abstractions.Smuggler;
using Raven.Studio.Commands;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Controls
{
	public partial class DeleteDatabase : ChildWindow
	{
		public ExportTaskSectionModel ExportTaskSectionModel;
		
		public DeleteDatabase()
		{
			ExportTaskSectionModel = new ExportTaskSectionModel();
			InitializeComponent();
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = false;
		}

        public string DatabaseName { get; set; }

		private async void Export_Click(object sender, RoutedEventArgs e)
		{
		    var exportTask = new ExportDatabaseTask(ApplicationModel.DatabaseCommands.ForDatabase(DatabaseName), DatabaseName,
		        includeAttachements: true, includeDocuments: true, includeIndexes: true,
		        removeAnalyzers: false,
		        includeTransformers: true, 
                shouldExcludeExpired: false, 
                batchSize: 512, 
                transformScript: "",
		        filterSettings: new List<FilterSetting>());
            exportTask.MessageOutput += (s, messageArgs) => exportLog.Text = messageArgs.Message + "\n" + exportLog.Text;

		    OKButton.IsEnabled = false;
		    CancelButton.IsEnabled = false;
		    ExportButton.IsEnabled = false;

            try
            {
                await exportTask.Run();
            }
            finally
            {
                OKButton.IsEnabled = true;
                CancelButton.IsEnabled = true;
                ExportButton.IsEnabled = true;
            }
		}
	}
}