using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;

namespace Raven.Tests.Util
{
	[CLSCompliant(false)]
	public abstract class ProcessDriver : CriticalFinalizerObject, IDisposable
	{
		protected Process _process;
		private string _name;

		protected BlockingCollection<string> output = new BlockingCollection<string>(new ConcurrentQueue<string>());
		protected BlockingCollection<string> error = new BlockingCollection<string>(new ConcurrentQueue<string>());
		protected BlockingCollection<string> input = new BlockingCollection<string>(new ConcurrentQueue<string>());

		protected void StartProcess(string exePath, string arguments = "")
		{
			_name = new FileInfo(exePath).Name;

			ProcessStartInfo psi = new ProcessStartInfo(exePath);
				
			psi.LoadUserProfile = false;

			psi.Arguments = arguments;
			psi.UseShellExecute = false;
			psi.RedirectStandardError = true;
			psi.RedirectStandardInput = true;
			psi.RedirectStandardOutput = true;
			psi.CreateNoWindow = true;

			_process = Process.Start(psi);

			Task.Factory.StartNew(() =>
			{
				string line;
				while ((line = _process.StandardOutput.ReadLine()) != null)
				{
					output.Add(line);
				}
			});

			Task.Factory.StartNew(() =>
			{
				string line;
				while ((line = _process.StandardError.ReadLine()) != null)
				{
					error.Add(line);
				}
			});

			Task.Factory.StartNew(() =>
			{
				while (_process.HasExited == false)
				{
					try
					{
						_process.StandardInput.WriteLine(input.Take());
					}
					catch (ObjectDisposedException)
					{
					}
				}
			});
		}

		protected virtual void Shutdown() { }

		public void Dispose()
		{
			Dispose(true);
		}

		private void Dispose(bool disposing)
		{
			if (disposing)
				input.Dispose();

			if (_process != null)
			{
				Shutdown();

				var toDispose = _process;
				_process = null;

				toDispose.Dispose();
			}
		}

		~ProcessDriver()
		{
			Dispose(false);
		}

		protected Match WaitForConsoleOutputMatching(string pattern, int msMaxWait = 10000, int msWaitInterval = 500)
		{
			DateTime t = SystemTime.UtcNow;

			var sb = new StringBuilder();
			Match match;
			while(true)
			{
				string nextLine;
				output.TryTake(out nextLine, 100);

				if (nextLine == null)
				{
					if ((SystemTime.UtcNow - t).TotalMilliseconds > msMaxWait)
						throw new TimeoutException("Timeout waiting for regular expression " + pattern + Environment.NewLine + sb);
					
					continue;
				}

				sb.AppendLine(nextLine);

				match = Regex.Match(nextLine, pattern);

				if (match.Success)
					break;
			}
			return match;
		}
	}
}
