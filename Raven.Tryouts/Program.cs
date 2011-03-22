using System;
using Raven.Client.Document;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			int i = 0;
			while (true)
			{
				new VeryBigResultSetRemote().CanGetVeryBigResultSetsEvenThoughItIsBadForYou();
				Console.WriteLine(++i);
			}
		}
	}
}
