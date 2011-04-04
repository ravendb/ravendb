using System.Reflection;

namespace Raven.Studio.Shell
{
	using Caliburn.Micro;

	public class ErrorViewModel : Screen
	{
		public MethodInfo CurrentErrorSource { get; set; }
		public static ErrorViewModel Current { get; set; }

		public ErrorViewModel()
		{
			DisplayName = "Error!";
		}

		public string Message { get; set; }
		public string Details { get; set; }

		protected override void OnDeactivate(bool close)
		{
			Current = null;
		}

		protected override void OnActivate()
		{
			Current = this;
		}
	}
}