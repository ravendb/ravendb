using System;
using Raven.Tests.Replication;

namespace Raven.Tryouts
{
	public class Program
	{
		public static void Main()
		{
			for (int i = 0; i < 1000; i++)
			{
				Console.WriteLine(i);
				using (var test = new IndexReplication())
					test.Replicate_all_indexes_should_respect_disable_indexing_flag();
			}
		}
	}
}
