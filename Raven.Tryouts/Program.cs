using System;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			while (true)
			{
				new VeryBigResultSet().CanGetVeryBigResultSetsEvenThoughItIsBadForYou();
				Console.WriteLine(DateTime.Now);
			}
		}
	}
}
