using System;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Client.Tests.Document;

namespace Raven.Tryouts
{
	internal class Program
	{
	

		public static void Main()
		{
			BasicConfigurator.Configure(new ConsoleAppender
			{
				Layout = new SimpleLayout()
			});
			try
			{
				new DocumentStoreEmbeddedTests().Should_retrieve_all_entities();
				Console.WriteLine("done");
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}

}