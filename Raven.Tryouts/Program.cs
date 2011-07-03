using System;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			//var appender = new ConsoleAppender
			//{
			//    Layout = new SimpleLayout()
			//};
			//appender.AddFilter(new LoggerMatchFilter
			//{
			//    AcceptOnMatch = true,
			//    LoggerToMatch = typeof(HttpServer).FullName
			//});
			//appender.AddFilter(new DenyAllFilter());

			//BasicConfigurator.Configure(appender);

			for (int i = 0; i < 8462; i++)
			{
				Console.WriteLine(i);
				new ManyDocumentsViaDTC().WouldBeIndexedProperly();

			}
		}
	}
}
