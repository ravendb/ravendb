using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client
{
	/// <summary>
	///     Query highlightings for the documents.
	/// </summary>
	public class FieldHighlightings
	{
		private readonly Dictionary<string,string[]> highlightings;

		public FieldHighlightings(string fieldName)
		{
			this.FieldName = fieldName;
			this.highlightings = new Dictionary<string, string[]>();
		}

		/// <summary>
		///     The field name.
		/// </summary>
		public string FieldName { get; private set; }

		public IEnumerable<string> ResultIndents
		{
			get { return this.highlightings.Keys; }
		}

		/// <summary>
		///     Returns the list of document's field highlighting fragments.
		/// </summary>
		/// <param name="documentId">The document id.</param>
		/// <returns></returns>
		public string[] GetFragments(string documentId)
		{
			string[] result;

			if (!this.highlightings.TryGetValue(documentId, out result))
				return new string[0];

			return result;
		}

		internal void Update(QueryResult queryResult)
		{
			this.highlightings.Clear();

			foreach (var entityFragments in queryResult.Highlightings)
				foreach (var fieldFragments in entityFragments.Value)
					if (fieldFragments.Key == this.FieldName)
						this.highlightings.Add(entityFragments.Key, fieldFragments.Value);
		}
	}
}