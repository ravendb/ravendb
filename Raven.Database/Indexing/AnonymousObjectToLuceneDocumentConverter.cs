using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;

namespace Raven.Database.Indexing
{
	public class AnonymousObjectToLuceneDocumentConverter
	{
		public IEnumerable<Field> Index(object val, PropertyDescriptorCollection properties)
		{
			return (from property in properties.Cast<PropertyDescriptor>()
			where property.Name != "__document_id"
			let value = property.GetValue(val)
			where value != null
			select new Field(property.Name, ToIndexableString(value), Field.Store.YES, Field.Index.TOKENIZED));
		}


		private static string ToIndexableString(object val)
		{
			if (val is DateTime)
				return DateTools.DateToString((DateTime) val, DateTools.Resolution.DAY);

			return val.ToString();
		}
	}
}