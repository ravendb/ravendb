using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Raven.Tests.Migration.Utils
{
	public class ServerRunner : IDisposable
	{
		public int ProcessId
		{
			get
			{
				if (dbServerProcess != null) 
					return dbServerProcess.Id;

				return -1;
			}
		}

		private readonly Process dbServerProcess;
		private readonly AutoResetEvent outputWaitHandle;
		private readonly AutoResetEvent errorWaitHandle;

		private readonly ManualResetEventSlim serverStartedEvent = new ManualResetEventSlim();

		private bool disposed;

		private const int TimeoutInSeconds = 20;

		public static ServerRunner Run(int port, string storageType, string standaloneServerExePath)
		{
			return new ServerRunner(port, storageType, standaloneServerExePath);
		}

		private ServerRunner(int port, string storageType, string standaloneServerExePath)
		{
			Console.WriteLine("Starting test with database on port " + port);
			dbServerProcess = new Process();
			dbServerProcess.StartInfo.FileName = standaloneServerExePath;
			dbServerProcess.StartInfo.Arguments = "--set=Raven/Port==" + port + " " + "--set=Raven/StorageTypeName==" + storageType + " " + "--set=Raven/AnonymousAccess==Admin";
			dbServerProcess.StartInfo.LoadUserProfile = false;
			dbServerProcess.StartInfo.UseShellExecute = false;
			dbServerProcess.StartInfo.RedirectStandardError = true;
			dbServerProcess.StartInfo.RedirectStandardInput = true;
			dbServerProcess.StartInfo.RedirectStandardOutput = true;
			dbServerProcess.StartInfo.CreateNoWindow = true;
			dbServerProcess.StartInfo.WorkingDirectory = Path.GetDirectoryName(standaloneServerExePath);

			outputWaitHandle = new AutoResetEvent(false);
			errorWaitHandle = new AutoResetEvent(false);

			dbServerProcess.OutputDataReceived += (sender, e) =>
			{
				if (e.Data == null)
				{
					outputWaitHandle.Set();
				}
				else
				{
					if (e.Data.StartsWith("Available commands:", StringComparison.InvariantCultureIgnoreCase) && serverStartedEvent.IsSet == false)
						serverStartedEvent.Set();
				}
			};
			dbServerProcess.ErrorDataReceived += (seder, e) =>
			{
				if (e.Data == null)
					errorWaitHandle.Set();
			};

			dbServerProcess.Start();
			dbServerProcess.BeginOutputReadLine();
			dbServerProcess.BeginErrorReadLine();

			WaitForStartAndCalculateWarmupTime();
		}

		private void WaitForStartAndCalculateWarmupTime()
		{
			var stopwatch = Stopwatch.StartNew();
			if (serverStartedEvent.Wait(TimeSpan.FromSeconds(TimeoutInSeconds)) == false)
				throw new InvalidOperationException("Server did not started withing 30 seconds.");

			stopwatch.Stop();
			StartupTime = stopwatch.Elapsed;
		}

		public TimeSpan StartupTime { get; private set; }

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		~ServerRunner()
		{
			Dispose(false);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposed)
				return;

			if (disposing)
			{
				try
				{
					dbServerProcess.StandardInput.Write("q\r\n");
				}
				catch (Exception)
				{
				}

				if (dbServerProcess.WaitForExit(TimeoutInSeconds * 1000) && outputWaitHandle.WaitOne(TimeSpan.FromSeconds(TimeoutInSeconds)) &&
					errorWaitHandle.WaitOne(TimeSpan.FromSeconds(TimeoutInSeconds)))
				{
					//process closed
				}
				else
					throw new Exception(string.Format("RavenDB command-line server did not halt within {0} seconds of pressing enter.", (TimeoutInSeconds / 1000)));

				dbServerProcess.Close();
			}
			disposed = true;
		}
	}
}