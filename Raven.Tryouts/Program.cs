using System;
using Raven.Tests.Core.Replication;

namespace Raven.Tryouts
{
	public class Program
	{
		private static void Main()
		{
			for (int i = 0; i < 1000; i++)
			{
				Console.WriteLine(i);
				using (var test = new IndexReplication())
				{
					test.Should_replicate_all_indexes_if_relevant_endpoint_is_hit();
				}
			}
		}

	}
}