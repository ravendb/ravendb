using System;
using Raven.Tests.Bugs.DTC;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			while (true)
			{
				try
				{
					Do();
					Console.WriteLine("Passed {0:#,#}", GC.GetTotalMemory(true));
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
				}
			}
		}

		private static void Do()
		{
			new UsingDTCForUpdates().can_update_a_doc_within_transaction_scope();
		}
	}
}
