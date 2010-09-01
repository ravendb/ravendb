using System;
using System.IO;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Tests.Indexes;
using Raven.Tests.Triggers;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			foreach (var file in Directory.GetFiles(".", "*.log"))
			{
				File.Delete(file);
			}

			for (var i = 0; i < 100; i++)
			{
				var file = i + ".log";
				Console.WriteLine(file);
				var fileAppender = new FileAppender
				{
					Layout = new PatternLayout(PatternLayout.DetailConversionPattern),
					File = file,
				};
				fileAppender.ActivateOptions();
				BasicConfigurator.Configure(fileAppender);
				try
				{
					using (var x = new ReadTriggers())
					{
						x.CanPageThroughFilteredQuery();
					}
					using (var x = new QueryingOnDefaultIndex())
					{
						x.CanPageOverDefaultIndex();
					}
				}
				finally
				{
					LogManager.Shutdown();
				}
			}
		}
	}
}