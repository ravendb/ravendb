using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Bundles.Tests.Authorization.Bugs;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			for (int i = 0; i < 1024; i++)
			{
				Console.WriteLine(i);
				using (var t = new LoadingSavedInfo())
				{
					t.BugWhenSavingDocumentWithPreviousAuthorization_WithQuery();
				}
			}
		}
	}
}
