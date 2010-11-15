using System;
using System.IO;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Scenarios;
using Raven.Tests.Indexes;
using Raven.Tests.Triggers;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
		    Environment.CurrentDirectory = @"C:\Work\ravendb\Raven.Scenarios";
		    Console.WriteLine("Starting...");
		    for (int i = 0; i < 1500; i++)
		    {
                new Scenario(
                @"C:\Work\ravendb\Raven.Scenarios\Scenarios\WhenDeletingDocsWillUpdateMapReduceIndex.saz"
                ).Execute();
                Console.Write(i + "\r");
		    }
		}
	}
}
