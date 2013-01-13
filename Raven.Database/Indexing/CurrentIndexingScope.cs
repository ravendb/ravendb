using System;
using System.Collections.Generic;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
	public class CurrentIndexingScope : IDisposable
	{
		private readonly Func<string, dynamic> loadDocument;
		private readonly Action<IDictionary<string, HashSet<string>>> onDispose;
		[ThreadStatic]
		private static CurrentIndexingScope current;

		public static CurrentIndexingScope Current
		{
			get { return current; }
			set { current = value; }
		}

		public CurrentIndexingScope(
			Func<string, dynamic> loadDocument,
			Action<IDictionary<string,HashSet<string>>> onDispose)
		{
			this.loadDocument = loadDocument;
			this.onDispose = onDispose;
		}

		public dynamic Source { get; set; }

		public IDictionary<string,HashSet<string>> ReferencedDocuments = new Dictionary<string, HashSet<string>>(StringComparer.InvariantCultureIgnoreCase); 

		public dynamic LoadDocument(string key)
		{
			if (key == null)
				return new DynamicNullObject();

			var source = Source;
			if (source == null)
				throw new ArgumentException(
					"LoadDocument can only be called as part of the Map stage of the index, but was called with " + key +
					" without a source.");
			var id = source.__document_id as string;
			if (string.IsNullOrEmpty(id))
				throw new ArgumentException(
					"LoadDocument can only be called as part of the Map stage of the index, but was called with " + key +
					" without a document. Current source: " + source);

			if (string.Equals(key, id))
				return source;

			HashSet<string> set;
			if(ReferencedDocuments.TryGetValue(id, out set) == false)
				ReferencedDocuments.Add(id, set = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase));
			set.Add(key);
			return loadDocument(key);
		}

		public void Dispose()
		{
			onDispose(ReferencedDocuments);
			current = null;
		}
	}
}