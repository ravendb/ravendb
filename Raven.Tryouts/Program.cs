using System;
using System.Threading;
using Raven.Client;
using Raven.Database.DiskIO;
using Raven.Json.Linq;
using System.Linq;
using Raven.Database.Extensions;
using Raven.SlowTests.Issues;
using Raven.Tests.FileSystem.ClientApi;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
			for (int i = 0; i < 100; i++)
			{
				using (var a = new RavenDB_1359())
				{
					Console.WriteLine(i);
					a.IndexThatLoadAttachmentsShouldIndexAllDocuments();
				}
			
			}

		}
	}


	
}