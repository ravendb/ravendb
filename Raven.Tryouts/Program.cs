using System;
using System.Collections.Generic;
using NLog;
using Voron.Tests.Journal;

namespace Raven.Tryouts
{
	class Program
	{

		private static void Main(string[] args)
		{
			using (var x = new LogShipping())
			{
				x.StorageEnvironment_should_be_able_to_accept_transactionsToShip_with_new_trees();
			}
		}
	}

}