using System;
using Raven.Tests.Indexes;
using Raven.Tests.Issues;
using Xunit;

namespace Raven.Tryouts
{
	class Program
	{
		private static void Main(string[] args)
		{
            new RavenDB_1754().ShouldntThrowCollectionModified();
		}
	}
}