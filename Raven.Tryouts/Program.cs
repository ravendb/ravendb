using System;
using Raven.Tests.Core;
using Raven.Tests.Core.Querying;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				using(var s = new TestServerFixture())
				using (var a = new Searching())
				{
					Console.WriteLine(i);
					a.SetFixture(s);
					a.CanProvideSuggestionsAndLazySuggestions();
				}
			
			}

		}
	}


	
}