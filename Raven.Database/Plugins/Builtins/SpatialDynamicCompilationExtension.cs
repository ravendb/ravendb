using System;
using Lucene.Net.Documents;
using Raven.Database.Indexing;

namespace Raven.Database.Plugins.Builtins
{
	public class SpatialDynamicCompilationExtension : AbstractDynamicCompilationExtension
	{
		public override string[] GetNamespacesToImport()
		{
			return new[]
			{
				typeof (SpatialIndex).Namespace
			};
		}

		public override string[] GetAssembliesToReference()
		{
			return new string[]
			{
				typeof (AbstractField).Assembly.Location
			};
		}
	}
}