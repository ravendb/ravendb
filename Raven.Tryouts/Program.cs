using System;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			for (int i = 0; i < 100000; i++)
			{
				Console.WriteLine(i);
				new MultipleResultsPerDocumentAndPaging().WhenOutputingMultipleResultsPerDocAndPagingWillGetCorrectSize();
			}
		}
	}
}
