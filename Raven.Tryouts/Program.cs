using System;
using System.IO;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Client.Tests.Bugs;
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
		    Console.WriteLine("Starting...");
		    for (int i = 0; i < 1500; i++)
		    {
                using(var t = new DocumentStoreServerTests())
                {
                    t.Can_create_index_using_linq_from_client_using_map_reduce();
                }
                Console.Write(i + "\r");
		    }
		}
	}
}
