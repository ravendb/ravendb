using System.Windows.Navigation;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Bundles
{
	public partial class BundlesView : PageView
	{
		public BundlesView()
		{
			InitializeComponent();
			MaxSize.Maximum = int.MaxValue;
			WarnSize.Maximum = int.MaxValue;
			MaxDocs.Maximum = int.MaxValue;
			WarnDocs.Maximum = int.MaxValue;
		}

		// Executes when the user navigates to this page.
		protected override void OnNavigatedTo(NavigationEventArgs e)
		{
		}

	}
}
