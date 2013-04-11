using System.Windows;
using System.Windows.Controls;
using Raven.Studio.Commands;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;

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

		private void Export_Click(object sender, RoutedEventArgs e)
		{
			Command.ExecuteCommand(new ExportDatabaseCommand(ExportTaskSectionModel, line => Execute.OnTheUI(() => exportLog.Text = line + "\n" + exportLog.Text)));
		}
	}
}