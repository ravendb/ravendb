using System;
using System.IO;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Client.Tests.Document;
using Raven.Client.Tests.Indexes;
using Raven.Tests.Indexes;
using Raven.Tests.Triggers;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
		    for (int i = 0; i < 1500; i++)
		    {
		        Console.Write(i+"\r");
                using (var t = new Game())
                {
                    t.WillNotGetDuplicatedResults();
                }
		    }
		}
	}
}
