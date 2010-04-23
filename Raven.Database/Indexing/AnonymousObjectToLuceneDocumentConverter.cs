using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;

namespace Raven.Database.Indexing
{
	public class AnonymousObjectToLuceneDocumentConverter
	{
		public IEnumerable<Field> Index(object val, PropertyDescriptorCollection properties, IndexDefinition indexDefinition, Field.Store defaultStorage)
		{
			return (from property in properties.Cast<PropertyDescriptor>()
			        let name = property.Name
			        where name != "__document_id"
					let value = property.GetValue(val)
			        where value != null
					select new Field(name, ToIndexableString(value, indexDefinition.GetIndex(name)), indexDefinition.GetStorage(name, defaultStorage), indexDefinition.GetIndex(name)));
		}


		private static string ToIndexableString(object val, Field.Index indexOptions)
		{
			if (indexOptions == Field.Index.UN_TOKENIZED)
				return val.ToString();

			if (val is DateTime)
				return DateTools.DateToString((DateTime) val, DateTools.Resolution.DAY);

			if (val is int)
				return NumberTools.LongToString((int) val);

			if (val is long)
				return NumberTools.LongToString((long) val);

			return val.ToString();
		}
	}
}