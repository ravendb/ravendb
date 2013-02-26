using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			for (int i = 0; i < 200; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				using(var x = new RavenDB_406())
				{
					x.ShouldServeFromCacheIfThereWasNoChange();
				}
			}
		}
	}

}