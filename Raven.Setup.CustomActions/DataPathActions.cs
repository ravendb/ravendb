namespace Raven.Setup.CustomActions
{
	using System;
	using System.Threading;
	using System.Windows.Forms;
	using Microsoft.Deployment.WindowsInstaller;

	public class DataPathActions
	{
		[CustomAction]
		public static ActionResult OpenDataDirDirectoryChooser(Session session)
		{
			try
			{
				var task = new Thread(() =>
				{
					var fileDialog = new FolderBrowserDialog { ShowNewFolderButton = true };
					if (fileDialog.ShowDialog() == DialogResult.OK)
					{
						session["RAVEN_DATA_DIR"] = fileDialog.SelectedPath;
					}
				});
				task.SetApartmentState(ApartmentState.STA);
				task.Start();
				task.Join();

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during OpenDataDirDirectoryChooser. Exception: " + ex);
				return ActionResult.Failure;
			}
		}

		[CustomAction]
		public static ActionResult OpenIndexesDirectoryChooser(Session session)
		{
			try
			{
				var task = new Thread(() =>
				{
					var fileDialog = new FolderBrowserDialog { ShowNewFolderButton = true };
					if (fileDialog.ShowDialog() == DialogResult.OK)
					{
						session["RAVEN_INDEX_DIR"] = fileDialog.SelectedPath;
					}
				});
				task.SetApartmentState(ApartmentState.STA);
				task.Start();
				task.Join();

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during OpenIndexesDirectoryChooser. Exception: " + ex);
				return ActionResult.Failure;
			}
		}

		[CustomAction]
		public static ActionResult OpenStorageLogsDirectoryChooser(Session session)
		{
			try
			{
				var task = new Thread(() =>
				{
					var fileDialog = new FolderBrowserDialog { ShowNewFolderButton = true };
					if (fileDialog.ShowDialog() == DialogResult.OK)
					{
						session["RAVEN_STORAGE_LOGS_DIR"] = fileDialog.SelectedPath;
					}
				});
				task.SetApartmentState(ApartmentState.STA);
				task.Start();
				task.Join();

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during OpenStorageLogsDirectoryChooser. Exception: " + ex);
				return ActionResult.Failure;
			}
		}

		[CustomAction]
		public static ActionResult OpenFsDataDirDirectoryChooser(Session session)
		{
			try
			{
				var task = new Thread(() =>
				{
					var fileDialog = new FolderBrowserDialog { ShowNewFolderButton = true };
					if (fileDialog.ShowDialog() == DialogResult.OK)
					{
						session["RAVENFS_DATA_DIR"] = fileDialog.SelectedPath;
					}
				});
				task.SetApartmentState(ApartmentState.STA);
				task.Start();
				task.Join();

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during OpenDataDirDirectoryChooser. Exception: " + ex);
				return ActionResult.Failure;
			}
		}

		[CustomAction]
		public static ActionResult OpenWorkingDirDirectoryChooser(Session session)
		{
			try
			{
				var task = new Thread(() =>
				{
					var fileDialog = new FolderBrowserDialog { ShowNewFolderButton = true };
					if (fileDialog.ShowDialog() == DialogResult.OK)
					{
						session["RAVEN_WORKING_DIR"] = fileDialog.SelectedPath;
					}
				});
				task.SetApartmentState(ApartmentState.STA);
				task.Start();
				task.Join();

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during OpenDataDirDirectoryChooser. Exception: " + ex);
				return ActionResult.Failure;
			}
		}
	}
}