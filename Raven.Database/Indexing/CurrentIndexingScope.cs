using System;
using System.Collections.Generic;
using Raven.Abstractions.Linq;

namespace Raven.Database.Indexing
{
	public class CurrentIndexingScope : IDisposable
	{
		private readonly Func<string, dynamic> loadDocument;
		private readonly Action<IDictionary<string, HashSet<string>>, IDictionary<string, HashSet<string>>> onDispose;
		[ThreadStatic]
		private static CurrentIndexingScope current;

		public static CurrentIndexingScope Current
		{
			get { return current; }
			set { current = value; }
		}

		public CurrentIndexingScope(
			Func<string, dynamic> loadDocument,
			Action<IDictionary<string, HashSet<string>>, IDictionary<string, HashSet<string>>> onDispose)
		{
			this.loadDocument = loadDocument;
			this.onDispose = onDispose;
		}

		public dynamic Source { get; set; }

		private readonly IDictionary<string, HashSet<string>> referencedDocuments = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
		private readonly IDictionary<string, HashSet<string>> missingReferencedDocuments = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
		private readonly IDictionary<string,dynamic> docsCache = new Dictionary<string, dynamic>(StringComparer.OrdinalIgnoreCase);

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
			if(referencedDocuments.TryGetValue(id, out set) == false)
				referencedDocuments.Add(id, set = new HashSet<string>(StringComparer.OrdinalIgnoreCase));
			set.Add(key);

			dynamic value;
			if (docsCache.TryGetValue(key, out value))
				return value;

			value = loadDocument(key);

            if (value == null)
            {
				HashSet<string> missingDocsSet;
				if (missingReferencedDocuments.TryGetValue(id, out missingDocsSet) == false)
					missingReferencedDocuments.Add(id, missingDocsSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase));

	            missingDocsSet.Add(key);
            }

			docsCache[key] = value;
			return value;
		}

		public void Dispose()
		{
			onDispose(referencedDocuments, missingReferencedDocuments);
			current = null;
		}
	}
}
