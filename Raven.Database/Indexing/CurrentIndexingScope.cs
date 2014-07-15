using System;
using System.Collections.Generic;
using Mono.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;

namespace Raven.Database.Indexing
{
	public class CurrentIndexingScope : IDisposable
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly DocumentDatabase database;
		private readonly string index;

		[ThreadStatic]
		private static CurrentIndexingScope current;

		public static CurrentIndexingScope Current
		{
			get { return current; }
			set { current = value; }
		}

		public CurrentIndexingScope(DocumentDatabase database, string index)
		{
		    this.database = database;
		    this.index = index;
		}

        public IDictionary<string, HashSet<string>> ReferencedDocuments
		{
			get { return referencedDocuments; }
		}
		public IDictionary<string, Etag> ReferencesEtags
		{
			get { return referencesEtags; }
		}

		public dynamic Source { get; set; }
		public IDictionary<string, HashSet<string>> ReferencedDocuments
		{
			get { return referencedDocuments; }
		}
		public IDictionary<string, Etag> ReferencesEtags
		{
			get { return referencesEtags; }
		}

		private readonly IDictionary<string, HashSet<string>> referencedDocuments = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
		private readonly IDictionary<string, Etag> referencesEtags = new Dictionary<string, Etag>();
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
			if(ReferencedDocuments.TryGetValue(id, out set) == false)
				ReferencedDocuments.Add(id, set = new HashSet<string>(StringComparer.OrdinalIgnoreCase));
			set.Add(key);

			dynamic value;
			if (docsCache.TryGetValue(key, out value))
				return value;

			var doc = database.Documents.Get(key, null);

			if (doc == null)
            {
			    log.Debug("Loaded document {0} by document {1} for index {2} could not be found", key, id, index);

				ReferencesEtags.Add(key, Etag.Empty);
				value = new DynamicNullObject();
            }
			else
			{
				log.Debug("Loaded document {0} with etag {3} by document {1} for index {2}\r\n{4}", key, id, index, doc.Etag, doc.ToJson());

				ReferencesEtags.Add(key, doc.Etag);
				value = new DynamicJsonObject(doc.ToJson());
			}

			docsCache[key] = value;
			return value;
		}

		public void Dispose()
		{
			current = null;
		}
	}
}
