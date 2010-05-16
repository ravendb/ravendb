using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;

namespace Raven.Database.Indexing
{
    public static class AnonymousObjectToLuceneDocumentConverter
	{
		public static IEnumerable<AbstractField> Index(object val, PropertyDescriptorCollection properties, IndexDefinition indexDefinition, Field.Store defaultStorage)
		{
			return (from property in properties.Cast<PropertyDescriptor>()
			        let name = property.Name
			        where name != "__document_id"
					let value = property.GetValue(val)
			        where value != null
					select Createfield(name, value, indexDefinition, defaultStorage));
		}

        public static IEnumerable<AbstractField> Index(JObject document, IndexDefinition indexDefinition, Field.Store defaultStorage)
        {
            return (from property in document.Cast<JProperty>()
                    let name = property.Name
                    where name != "__document_id"
                    let value = GetPropertyValue(property)
                    where value != null
                    select Createfield(name, value, indexDefinition, defaultStorage));
        }

        private static object GetPropertyValue(JProperty property)
        {
            switch (property.Value.Type)
            {
                case JsonTokenType.Array:
                case JsonTokenType.Object:
                    return property.Value.ToString(Formatting.None);
                default:
                    return property.Value.Value<object>();
            }
        }

        private static AbstractField Createfield(string name, object value, IndexDefinition indexDefinition, Field.Store defaultStorage)
		{
			if (indexDefinition.GetIndex(name, Field.Index.ANALYZED) == Field.Index.NOT_ANALYZED || value is string)
				return new Field(name, value.ToString(), indexDefinition.GetStorage(name, defaultStorage),
								 indexDefinition.GetIndex(name, Field.Index.ANALYZED));

			if (value is int)
			{
				return new Field(name, JsonLuceneNumberConverter.NumberToString((int)value), indexDefinition.GetStorage(name, defaultStorage),
								 indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED));

			}
			if (value is long)
			{
				return new Field(name, JsonLuceneNumberConverter.NumberToString((long)value), indexDefinition.GetStorage(name, defaultStorage),
								 indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED));

			}
            if(value is decimal)
            {
                return new Field(name, JsonLuceneNumberConverter.NumberToString((decimal)value), indexDefinition.GetStorage(name, defaultStorage),
                                 indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED));
            }
            if (value is float)
            {
                return new Field(name, JsonLuceneNumberConverter.NumberToString((float)value), indexDefinition.GetStorage(name, defaultStorage),
                                 indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED));
            }
            if (value is double)
            {
                return new Field(name, JsonLuceneNumberConverter.NumberToString((double)value), indexDefinition.GetStorage(name, defaultStorage),
                                 indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED));
            }
			if (value is DateTime)
			{
				return new Field(name, DateTools.DateToString((DateTime)value, DateTools.Resolution.MILLISECOND),
					indexDefinition.GetStorage(name, defaultStorage),
					indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED));
			}
			return new Field(name, value.ToString(), indexDefinition.GetStorage(name, defaultStorage),
							 indexDefinition.GetIndex(name, Field.Index.ANALYZED));
		}
	}
}