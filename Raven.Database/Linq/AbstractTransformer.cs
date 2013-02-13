using System;
using System.Collections.Generic;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;

namespace Raven.Database.Linq
{
	public abstract class AbstractTransformer
	{
		private TransformerDefinition transformerDefinition;
		public IndexingFunc TransformResultsDefinition { get; set; }
		public string SourceCode { get; set; }

		protected dynamic LoadDocument(string key)
		{
			if (CurrentIndexingScope.Current == null)
				throw new InvalidOperationException("LoadDocument may only be called from the map portion of the index. Was called with: " + key);

			return CurrentIndexingScope.Current.LoadDocument(key);
		}

		protected IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
		{
			return new RecursiveFunction(item, func).Execute();
		}

		public void Init(TransformerDefinition def)
		{
			transformerDefinition = def;
		}
	}
}