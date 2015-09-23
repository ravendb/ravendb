using System;
using System.Diagnostics;
using System.IO;

namespace Raven.Tests.Common.Util
{
	public class IISExpressDriver : ProcessDriver
	{
		private readonly bool run64Bit;

		public IISExpressDriver(bool run64Bit)
		{
			this.run64Bit = run64Bit;
		}

		public string Url { get; private set;  }

		public void Start(string physicalPath, int port)
		{
			var sitePhysicalDirectory = physicalPath;

			foreach (var process in Process.GetProcessesByName("iisexpress"))
			{
				process.Kill();
			}

			var processFileName = run64Bit
				? @"c:\program files\IIS Express\IISExpress.exe" 
				: @"c:\program files (x86)\IIS Express\IISExpress.exe";

			StartProcess(processFileName, @"/systray:false /trace:error /port:" + port + @" /path:" + sitePhysicalDirectory);

			var match = WaitForConsoleOutputMatching(@"Successfully registered URL ""([^""]*)""");

			Url = match.Groups[1].Value;
		}

		protected override void Shutdown()
		{
			try
			{
				_process.Kill();
			}
			catch (Exception)
			{
			}

			if (!_process.WaitForExit(10000))
				throw new Exception("IISExpress did not halt within 10 seconds.");
		}
	}
}
