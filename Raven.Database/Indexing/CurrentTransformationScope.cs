using System;
using Raven.Database.Impl;

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