using System.IO;

namespace Raven.Setup.CustomActions
{
	using System.Threading;
	using System.Windows.Forms;
	using Microsoft.Deployment.WindowsInstaller;

	public class CustomActions
	{
		[CustomAction]
		public static ActionResult OpenLicenseFileChooser(Session session)
		{
			session.Log("Begin OpenLicenseFileChooser Custom Action");

			var task = new Thread(() => GetFile(session));
			task.SetApartmentState(ApartmentState.STA);
			task.Start();
			task.Join();

			session.Log("End OpenLicenseFileChooser Custom Action");

			return ActionResult.Success;
		}

		[CustomAction]
		public static ActionResult LicenseFileExists(Session session)
		{
			session.Log("Begin LicenseFileExists Custom Action");

			var task = new Thread(() =>
			{
				session["LICENSE_FILE_EXISTS"] = File.Exists(session["RAVEN_LICENSE_FILE_PATH"]).ToString();
			});
			task.SetApartmentState(ApartmentState.STA);
			task.Start();
			task.Join();

			session.Log("End LicenseFileExists Custom Action");

			return ActionResult.Success;
		}

		private static void GetFile(Session session)
		{
			var fileDialog = new OpenFileDialog { Filter = "License File (*.xml)|*.xml" };
			if (fileDialog.ShowDialog() == DialogResult.OK)
			{
				session["RAVEN_LICENSE_FILE_PATH"] = fileDialog.FileName;
			}
		}
	}
}
