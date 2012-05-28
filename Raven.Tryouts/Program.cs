using System;
using System.Diagnostics;
using Raven.Client.Document;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			using(var docStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				Console.WriteLine("Ready...");
				for (int i = 0; i < 1000; i++)
				{
					Console.ReadLine();
					using (var session = docStore.OpenSession())
					{
						var sp = Stopwatch.StartNew();
						var user = session.Load<dynamic>("users/ayende");
						Console.WriteLine("{0} - {1} - {2}", i, user.Name, sp.ElapsedMilliseconds);
					}
				}
			}
		} 
	}
}