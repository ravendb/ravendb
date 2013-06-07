using System;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Issues;

class Program
{
	static void Main(string[] args)
	{
		DocumentDatabase.Restore(new RavenConfiguration(), @"C:\Users\Ayende\AppData\Local\Temp\2013-04-01\2013-04-01", 
			@"C:\Users\Ayende\AppData\Local\Temp\2013-04-01\2013-04-01.restored", Console.WriteLine, false);
	}
}
