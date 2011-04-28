using System;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Tests.Indexes;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			//BasicConfigurator.Configure(new ConsoleAppender
			//{
			//    Layout = new SimpleLayout()
			//});
		    for (int i = 0; i < 100; i++)
		    {
		    	Console.WriteLine(i);
				using (var compiledIndex = new CompiledIndex())
					compiledIndex.CompileIndexWillTurnEventsToAggregate();
		    }
		}
	}
}
