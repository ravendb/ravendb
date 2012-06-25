using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Impl;

namespace Raven.Database.Indexing
{
	public class FieldsToFetch
	{
		private readonly string additionalField;
		private readonly HashSet<string> fieldsToFetch;
		private readonly AggregationOperation aggregationOperation;
		private HashSet<string > ensuredFieldNames;
		public bool FetchAllStoredFields { get; set; }

		public FieldsToFetch(string[] fieldsToFetch, AggregationOperation aggregationOperation, string additionalField)
		{
			this.additionalField = additionalField;
			if (fieldsToFetch != null)
			{
				this.fieldsToFetch = new HashSet<string>(fieldsToFetch);
				FetchAllStoredFields = this.fieldsToFetch.Remove(Constants.AllFields);
			}
			this.aggregationOperation = aggregationOperation.RemoveOptionals();

			if (this.aggregationOperation != AggregationOperation.None)
				EnsureHasField(this.aggregationOperation.ToString());
			
			IsDistinctQuery = aggregationOperation.HasFlag(AggregationOperation.Distinct) &&
							  fieldsToFetch != null && fieldsToFetch.Length > 0;
			
			IsProjection = fieldsToFetch != null && fieldsToFetch.Length != 0;
		
			if(IsProjection && IsDistinctQuery == false)
				EnsureHasField(additionalField);
		}

		public bool IsDistinctQuery { get; private set; }

		public bool IsProjection { get; private set; }


		public IEnumerable<string> Fields
		{
			get
			{
				HashSet<string> fieldsWeMustReturn = ensuredFieldNames == null
				                                     	? new HashSet<string>()
				                                     	: new HashSet<string>(ensuredFieldNames);
				foreach (var fieldToReturn in GetFieldsToReturn())
				{
					fieldsWeMustReturn.Remove(fieldToReturn);
					yield return fieldToReturn;
				}

				foreach (var field in fieldsWeMustReturn)
				{
					yield return field;
				}
			}
		}

		private IEnumerable<string> GetFieldsToReturn()
		{
			if (fieldsToFetch == null)
				yield break;
			foreach (var field in fieldsToFetch)
			{
				yield return field;
			}
		}


		public FieldsToFetch CloneWith(string[] newFieldsToFetch)
		{
			return new FieldsToFetch(newFieldsToFetch, aggregationOperation, additionalField);
		}

		public void EnsureHasField(string ensuredFieldName)
		{
			if (ensuredFieldNames == null)
				ensuredFieldNames = new HashSet<string>();
			ensuredFieldNames.Add(ensuredFieldName);
		}
	}
}