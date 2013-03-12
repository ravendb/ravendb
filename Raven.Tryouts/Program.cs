using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using System.Linq;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{

			var docStore = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "Users",
			};
			docStore.Initialize();
			while (true)
			{
				Console.ReadLine();
				using (docStore.AggressivelyCache())
				{
					using (var s = docStore.OpenSession())
					{
						var load = s.Advanced.LuceneQuery<dynamic>("dynamic/Users")
									.SelectFields<string>("Name")
						            .ToList();
						Console.WriteLine(string.Join(" ", load));
					}

					using (var s = docStore.OpenSession())
					{
						var load = s.Load<dynamic>("users/ayende");
						Console.WriteLine(load.Name);
					}
				}
			}
		} 
	}
}