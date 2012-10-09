using System.Windows.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Views
{
	public partial class Databases : PageView
	{
		public Databases()
		{
			InitializeComponent();
		}

	    private void EnterClick(object sender, KeyEventArgs e)
	    {
	        if(e.Key == Key.Enter)
	        {
	            var model = DataContext as Observable<DatabasesListModel>;

                if (model == null)
                    return;

	            model.Value.SearchApiKeys = SearchBox.Text;
                Command.ExecuteCommand(model.Value.Search);
	        }
	    }
	}
}