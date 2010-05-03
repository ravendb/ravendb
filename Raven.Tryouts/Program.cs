using System;
using System.IO;
using System.Threading;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Indexing;

namespace Raven.Tryouts
{
	internal class Program
	{
	

		public static void Main()
		{
			
			try
			{
				var ravenConfiguration = new RavenConfiguration
				{
					DataDirectory = @"C:\Work\StackOverflow.Data"
				};
				using (var db = new DocumentDatabase(ravenConfiguration))
				{
					db.Backup("bak");
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}

}