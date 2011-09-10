using System.Windows;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class ApplicationModel
	{
		public static ApplicationModel Current { get; private set; }

		static ApplicationModel()
		{
			Current = new ApplicationModel();
		}

		public ApplicationModel()
		{
			Server = new Observable<ServerModel>();
			var serverModel = new ServerModel();
			serverModel.Initialize()
				.ContinueOnSuccess(() => Server.Value = serverModel);
		}

		public Observable<ServerModel> Server { get; set; }

		public void SetupRootVisual(FrameworkElement rootVisual)
		{
			rootVisual.DataContext = this;
		}
	}
}