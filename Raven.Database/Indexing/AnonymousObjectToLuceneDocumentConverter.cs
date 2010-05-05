using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Util;

namespace Raven.Database.Indexing
{
	public class AnonymousObjectToLuceneDocumentConverter
	{
		public IEnumerable<AbstractField> Index(object val, PropertyDescriptorCollection properties, IndexDefinition indexDefinition, Field.Store defaultStorage)
		{
			return (from property in properties.Cast<PropertyDescriptor>()
			        let name = property.Name
			        where name != "__document_id"
					let value = property.GetValue(val)
			        where value != null
					select Createfield(name, value, indexDefinition, defaultStorage));
		}

		private static AbstractField Createfield(string name, object value, IndexDefinition indexDefinition, Field.Store defaultStorage)
		{
			if (indexDefinition.GetIndex(name) == Field.Index.NOT_ANALYZED || value is string)
				return new Field(name, value.ToString(), indexDefinition.GetStorage(name, defaultStorage),
								 indexDefinition.GetIndex(name));


			if (value is int)
			{
				return new Field(name, NumberTools.LongToString((int)value), indexDefinition.GetStorage(name, defaultStorage),
								 indexDefinition.GetIndex(name));

			}
			if (value is long)
			{
				return new Field(name, NumberTools.LongToString((long)value), indexDefinition.GetStorage(name, defaultStorage),
								 indexDefinition.GetIndex(name));

			}
			if (value is DateTime)
			{
				return new Field(name, DateTools.DateToString((DateTime)value, DateTools.Resolution.MILLISECOND),
					indexDefinition.GetStorage(name, defaultStorage),
					indexDefinition.GetIndex(name));
			}
			// Using the approach below result in failing test because we can't sort on ints using NumericField
			// Not really sure why

			//if(value is int)
			//{
			//    var i = ((int) value);
			//    var numericField = new NumericField(name);
			//    numericField.SetIntValue(i);
			//    return numericField;
			//}
			//if(value is long)
			//{
			//    var l = ((long)value);
			//    var numericField = new NumericField(name);
			//    numericField.SetLongValue(l);
			//    return numericField;
			//}
			
			//if(value is double)
			//{
			//    var d = ((double)value);
			//    var numericField = new NumericField(name);
			//    numericField.SetDoubleValue(d);
			//    return numericField;
			//}
			//if (value is float)
			//{
			//    var f = ((float)value);
			//    var numericField = new NumericField(name);
			//    numericField.SetFloatValue(f);
			//    return numericField;
			//}
			return new Field(name, value.ToString(), indexDefinition.GetStorage(name, defaultStorage),
							 indexDefinition.GetIndex(name));
		}
	}
}