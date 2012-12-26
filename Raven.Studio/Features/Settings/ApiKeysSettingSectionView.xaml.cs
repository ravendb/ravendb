using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
	public partial class ApiKeysSettingSectionView : UserControl
	{
		public ApiKeysSettingSectionView()
		{
			InitializeComponent();
		}

		private void EnterClick(object sender, KeyEventArgs e)
		{
			if (e.Key == Key.Enter)
			{
				var model = DataContext as Observable<ApiKeysSectionModel>;

				if (model == null)
					return;

				model.Value.SearchApiKeys = SearchBox.Text;
				Command.ExecuteCommand(model.Value.Search);
			}
		}
	}
}