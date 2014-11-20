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
			using (var a = new FileSessionListenersTests())
			{
				a.ConflictListeners_RemoteVersion();
			}
			

		}
	}


	
}