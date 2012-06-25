using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client;

namespace Raven.Bundles.Tests
{
	public class TestUtil
	{
		public static void WaitForIndexing(IDocumentStore store)
		{
			while (store.DatabaseCommands.GetStatistics().StaleIndexes.Length > 0)
			{
				Thread.Sleep(100);
			}
		}
	}
}
