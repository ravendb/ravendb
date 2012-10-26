using System;
using Raven.Abstractions.Indexing;
using Raven.Database.Linq;
using Raven.Tests.Faceted;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Write("\r"+i);
				using(var x = new FacetedIndexLimit())
				{
					x.CanPerformFacetedLimitSearch_HitsDesc();
				}
			}
		}
	}
}