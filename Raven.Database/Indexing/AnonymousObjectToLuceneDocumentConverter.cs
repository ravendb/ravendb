//-----------------------------------------------------------------------
// <copyright file="AnonymousObjectToLuceneDocumentConverter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	public class AnonymousObjectToLuceneDocumentConverter
	{
		private readonly IndexDefinition indexDefinition;
		private readonly List<int> multipleItemsSameFieldCount = new List<int>();
		private readonly Dictionary<FieldCacheKey, Field> fieldsCache = new Dictionary<FieldCacheKey, Field>();
		private readonly Dictionary<FieldCacheKey, NumericField> numericFieldsCache = new Dictionary<FieldCacheKey, NumericField>();

		public AnonymousObjectToLuceneDocumentConverter(IndexDefinition indexDefinition)
		{
			this.indexDefinition = indexDefinition;
		}

		public IEnumerable<AbstractField> Index(object val, PropertyDescriptorCollection properties, Field.Store defaultStorage)
		{
			return (from property in properties.Cast<PropertyDescriptor>()
			        let name = property.Name
					where name != Constants.DocumentIdFieldName
			        let value = property.GetValue(val)
					from field in CreateFields(name, value, defaultStorage)
			        select field);
		}

		public IEnumerable<AbstractField> Index(RavenJObject document, Field.Store defaultStorage)
		{
			return (from property in document
			        let name = property.Key
					where name != Constants.DocumentIdFieldName
			        let value = GetPropertyValue(property.Value)
			        from field in CreateFields(name, value, defaultStorage)
			        select field);
		}

		private static object GetPropertyValue(RavenJToken property)
		{
			switch (property.Type)
			{
				case JTokenType.Array:
				case JTokenType.Object:
					return property.ToString(Formatting.None);
				default:
					return property.Value<object>();
			}
		}

		/// <summary>
		/// This method generate the fields for indexing documents in lucene from the values.
		/// Given a name and a value, it has the following behavior:
		/// * If the value is enumerable, index all the items in the enumerable under the same field name
		/// * If the value is null, create a single field with the supplied name with the unanalyzed value 'NULL_VALUE'
		/// * If the value is string or was set to not analyzed, create a single field with the supplied name
		/// * If the value is date, create a single field with millisecond precision with the supplied name
		/// * If the value is numeric (int, long, double, decimal, or float) will create two fields:
		///		1. with the supplied name, containing the numeric value as an unanalyzed string - useful for direct queries
		///		2. with the name: name +'_Range', containing the numeric value in a form that allows range queries
		/// </summary>
		public IEnumerable<AbstractField> CreateFields(string name, object value, Field.Store defaultStorage, bool nestedArray = false)
		{
			if(string.IsNullOrWhiteSpace(name))
				throw new ArgumentException("Field must be not null, not empty and cannot contain whitespace", "name");

			if (char.IsLetter(name[0]) == false &&
				name[0] != '_')
			{
				name = "_" + name;
			}

			var storage = indexDefinition.GetStorage(name, defaultStorage);
			if (value == null)
			{
				yield return CreateFieldWithCaching(name, Constants.NullValue, storage,
								 Field.Index.NOT_ANALYZED_NO_NORMS);
				yield break;
			}
			if(Equals(value, string.Empty))
			{
				yield return CreateFieldWithCaching(name, Constants.EmptyString, storage,
							 Field.Index.NOT_ANALYZED_NO_NORMS);
				yield break;
			}
			if (value is DynamicNullObject)
			{
				if(((DynamicNullObject)value).IsExplicitNull)
				{
					var sortOptions = indexDefinition.GetSortOption(name);
					if(sortOptions != null && sortOptions.Value != SortOptions.None)
					{
						yield break; // we don't emit null for sorting	
					}
					yield return CreateFieldWithCaching(name, Constants.NullValue, storage,
					                                    Field.Index.NOT_ANALYZED_NO_NORMS);
				}
				yield break;
			}
			var boostedValue = value as BoostedValue;
			if (boostedValue != null)
			{
				foreach (var field in CreateFields(name, boostedValue.Value, storage))
				{
					field.SetBoost(boostedValue.Boost);
					field.SetOmitNorms(false);
					yield return field;
				}
				yield break;
			}

			var abstractField = value as AbstractField;
			if (abstractField != null)
			{
				yield return abstractField;
				yield break;
			}
			var bytes = value as byte[];
			if (bytes != null)
			{
				yield return CreateBinaryFieldWithCaching(name, bytes, storage);
				yield break;
			}

			var itemsToIndex = value as IEnumerable;
			if( itemsToIndex != null && ShouldTreatAsEnumerable(itemsToIndex))
			{
				var sentArrayField = false;
				int count = 1;
				foreach (var itemToIndex in itemsToIndex)
				{
					if (nestedArray == false && !Equals(storage, Field.Store.NO) && sentArrayField == false)
					{
						sentArrayField = true;
						yield return new Field(name + "_IsArray", "true", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
					}
				
					multipleItemsSameFieldCount.Add(count++);
					foreach (var field in CreateFields(name, itemToIndex, storage, nestedArray: true))
					{
						yield return field;
					}
					multipleItemsSameFieldCount.RemoveAt(multipleItemsSameFieldCount.Count - 1);
				}
				yield break;
			}

			var fieldIndexingOptions = indexDefinition.GetIndex(name, null);
			if (Equals(fieldIndexingOptions, Field.Index.NOT_ANALYZED) ||
			    Equals(fieldIndexingOptions, Field.Index.NOT_ANALYZED_NO_NORMS))// explicitly not analyzed
			{
				// date time and date time offset have the same structure fo analyzed and not analyzed.
				if (!(value is DateTime) && !(value is DateTimeOffset))
				{
					yield return CreateFieldWithCaching(name, value.ToString(), storage,
					                                    indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED_NO_NORMS));
					yield break;
				}
			}
			if (value is string) 
			{
			    var index = indexDefinition.GetIndex(name, Field.Index.ANALYZED);
				yield return CreateFieldWithCaching(name, value.ToString(), storage,
								 index); 
				yield break;
			}

			if (value is DateTime)
			{
				var val = (DateTime)value;
				yield return CreateFieldWithCaching(name, val.ToString(Default.DateTimeFormatsToWrite), storage,
						   indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED_NO_NORMS));
			}
			else if (value is DateTimeOffset)
			{
				var val = (DateTimeOffset)value;
				yield return CreateFieldWithCaching(name, val.UtcDateTime.ToString(Default.DateTimeFormatsToWrite), storage,
						   indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED_NO_NORMS));
			}
			else if(value is bool)
			{
				yield return new Field(name, ((bool) value) ? "true" : "false", storage,
							  indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED_NO_NORMS));

			}
			else if(value is IConvertible) // we need this to store numbers in invariant format, so JSON could read them
			{
				var convert = ((IConvertible) value);
				yield return CreateFieldWithCaching(name, convert.ToString(CultureInfo.InvariantCulture), storage,
									   indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED_NO_NORMS));
			}
			else if (value is IDynamicJsonObject)
			{
				var inner = ((IDynamicJsonObject)value).Inner;
				yield return CreateFieldWithCaching(name + "_ConvertToJson", "true", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
				yield return CreateFieldWithCaching(name, inner.ToString(), storage,
									   indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED_NO_NORMS));
			}
			else 
			{
				yield return CreateFieldWithCaching(name + "_ConvertToJson", "true", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
				yield return CreateFieldWithCaching(name, RavenJToken.FromObject(value).ToString(), storage,
									   indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED_NO_NORMS));
			}


			foreach (var numericField in CreateNumericFieldWithCaching(name, value, storage)) 
				yield return numericField;
		}

		private IEnumerable<AbstractField> CreateNumericFieldWithCaching(string name, object value,
			Field.Store defaultStorage)
		{

			var fieldName = name + "_Range";
			var storage = indexDefinition.GetStorage(name, defaultStorage);
			var cacheKey = new FieldCacheKey(name, null, storage, multipleItemsSameFieldCount.ToArray());
			NumericField numericField;
			if (numericFieldsCache.TryGetValue(cacheKey, out numericField) == false)
			{
				numericFieldsCache[cacheKey] = numericField = new NumericField(fieldName, storage, true);
			}

			if (value is int)
			{
				if (indexDefinition.GetSortOption(name) == SortOptions.Long)
					yield return numericField.SetLongValue((int) value);
				else
					yield return numericField.SetIntValue((int) value);
			}
			if (value is long)
			{
				yield return numericField
					.SetLongValue((long) value);
			}
			if (value is decimal)
			{
				yield return numericField
					.SetDoubleValue((double) (decimal) value);
			}
			if (value is float)
			{
				if (indexDefinition.GetSortOption(name) == SortOptions.Double)
					yield return numericField.SetDoubleValue((float) value);
				else
					yield return numericField.SetFloatValue((float) value);
			}
			if (value is double)
			{
				yield return numericField
					.SetDoubleValue((double) value);
			}
		}

		public static bool ShouldTreatAsEnumerable(object itemsToIndex)
	    {
			if (itemsToIndex == null)
				return false;

			if (itemsToIndex is DynamicJsonObject)
				return false;

			if (itemsToIndex is string)
				return false;

			if (itemsToIndex is RavenJObject)
				return false;

			if (itemsToIndex is IDictionary)
				return false;

	        return true;
	    }

		private Field CreateBinaryFieldWithCaching(string name, byte[] value, Field.Store store)
		{
			var cacheKey = new FieldCacheKey(name, null, store, multipleItemsSameFieldCount.ToArray());
			Field field;
			if (fieldsCache.TryGetValue(cacheKey, out field) == false)
			{
				fieldsCache[cacheKey] = field = new Field(name, value, store);
			}
			field.SetValue(value);
			field.SetBoost(1);
			field.SetOmitNorms(true);
			return field;
		}

		public class FieldCacheKey
		{
			private readonly string name;
			private readonly Field.Index index;
			private readonly Field.Store store;
			private readonly int[] multipleItemsSameField;

			public FieldCacheKey(string name, Field.Index index, Field.Store store, int[] multipleItemsSameField)
			{
				this.name = name;
				this.index = index;
				this.store = store;
				this.multipleItemsSameField = multipleItemsSameField;
			}


			protected bool Equals(FieldCacheKey other)
			{
				return string.Equals(name, other.name) && Equals(index, other.index) && Equals(store, other.store) &&
					multipleItemsSameField.SequenceEqual(other.multipleItemsSameField);
			}

			public override bool Equals(object obj)
			{
				if (ReferenceEquals(null, obj)) return false;
				if (ReferenceEquals(this, obj)) return true;
				if (obj.GetType() != typeof (FieldCacheKey)) return false;
				return Equals((FieldCacheKey) obj);
			}

			public override int GetHashCode()
			{
				unchecked
				{
					int hashCode = (name != null ? name.GetHashCode() : 0);
					hashCode = (hashCode*397) ^ (index != null ? index.GetHashCode() : 0);
					hashCode = (hashCode*397) ^ (store != null ? store.GetHashCode() : 0);
					hashCode = multipleItemsSameField.Aggregate(hashCode, (h, x) => h*397 ^ x);
					return hashCode;
				}
			}
		}

		private Field CreateFieldWithCaching(string name, string value, Field.Store store, Field.Index index)
		{
			var cacheKey = new FieldCacheKey(name, index, store, multipleItemsSameFieldCount.ToArray());
			Field field;
			if (fieldsCache.TryGetValue(cacheKey, out field) == false)
			{
				fieldsCache[cacheKey] = field = new Field(name, value, store, index);
			}
			field.SetValue(value);
			field.SetBoost(1);
			field.SetOmitNorms(true);
			return field;
		}

	}
}
