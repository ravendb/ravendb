using System;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			for (int i = 0; i < 1000; i++)
			{
				Console.WriteLine(i);
				new MultipleResultsPerDocumentAndPaging().WhenOutputingMultipleResultsPerDocAndPagingWillGetCorrectSize();
			}
		}
	}
}
