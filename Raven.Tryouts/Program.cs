using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Util;
using Raven.Client.Counters;
using Raven.Tests.Core;
using Raven.Tests.Core.BulkInsert;

namespace Raven.Tryouts
{
	public class Program
	{
		public static void Main()
		{			
			for (int i = 0; i < 1000; i++)
			{
				try
				{
					//if(i % 100 == 0)
						Console.WriteLine(i);
					using (var f = new TestServerFixture())
					{
						var test = new ChunkedBulkInsert();
						test.SetFixture(f);
						test.ValidateChunkedBulkInsertOperationsIDsCount();
					}
				}
				catch (Exception e)
				{
					Debugger.Break();
				}
			}
		}
	}
}
