using System;
using Raven.SlowTests.Issues;
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
				using (var s = new RavenDB_1359())
				{
					Console.WriteLine(i);
					s.IndexThatLoadAttachmentsShouldIndexAllDocuments();
				}
			
			}

		}
	}


	
}