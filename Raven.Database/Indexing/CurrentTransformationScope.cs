using System;
using System.Collections.Generic;
using Raven.Database.Impl;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class CurrentTransformationScope : IDisposable
	{
		private readonly DocumentDatabase database;
		private readonly DocumentRetriever retriever;

		[ThreadStatic] private static CurrentTransformationScope current;
		private CurrentTransformationScope old;

		public static CurrentTransformationScope Current
	    {
			get { return current; }
	    }

	    public CurrentTransformationScope(DocumentDatabase database, DocumentRetriever documentRetriever)
	    {
		    this.database = database;
		    retriever = documentRetriever;
		    old = current;
			current = this;
			
	    }

		public void Dispose()
		{
			current = old;
		}

		public DocumentDatabase Database
		{
			get { return database; }
		}
		public DocumentRetriever Retriever
		{
			get { return retriever; }
		}
	}
}