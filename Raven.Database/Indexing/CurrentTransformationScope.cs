using System;
using System.Collections.Generic;
using Raven.Database.Impl;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class CurrentTransformationScope : IDisposable
	{
		[ThreadStatic]
		private static DocumentRetriever current;

	    public static DocumentRetriever Current
	    {
	        get { return current; }
	    }

	    public CurrentTransformationScope(DocumentRetriever documentRetriever)
		{
			current = documentRetriever;
		}

		public void Dispose()
		{
			current = null;
		}
	}
}