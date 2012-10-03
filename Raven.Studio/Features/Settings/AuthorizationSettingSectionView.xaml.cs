using System.Windows.Controls;
using System.Windows.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
    public partial class AuthorizationSettingSectionView : UserControl
    {
        public AuthorizationSettingSectionView()
        {
            InitializeComponent();
        }

        private void EnterClick(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Enter)
            {
                var model = DataContext as AuthorizationSettingsSectionModel;

                if (model == null)
                    return;

                model.SearchUsers = SearchBox.Text;
                Command.ExecuteCommand(model.Search);
            }
        }
    }
}