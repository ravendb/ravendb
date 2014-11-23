using System;
using System.Threading;
using Raven.Client;
using Raven.Database.DiskIO;
using Raven.Json.Linq;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Tests.FileSystem.ClientApi;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
			for (int i = 0; i < 100; i++)
			{
				using (var a = new FileSessionListenersTests())
				{
					Console.WriteLine(i);
					a.ConflictListeners_RemoteVersion().Wait();
				}
			
			}

		}
	}


	
}