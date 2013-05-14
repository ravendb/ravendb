using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class IndexedPropertiesSetupDoc
	{
		public static string IdPrefix = "Raven/IndexedProperties/";
		public string DocumentKey { get; set; }
        
        public IndexedPropertiesType Type { get; set; }

        IDictionary<string, string> _fieldNameMappings;
		public IDictionary<string,string> FieldNameMappings 
        { 
            get { return _fieldNameMappings; }
            set
            {
                if (value != null)
                {
                    _fieldNameMappings = value;
                    Type = IndexedPropertiesType.FieldMapping;
                }
            }
        }

        string _script;
        public string Script
        {
            get { return _script; }
            set
            {
                if (value != null)
                {
                    _script = value;
                    Type = IndexedPropertiesType.Scripted;
                }
            }
        }

        public string CleanupScript { get; set; }
        
		public IndexedPropertiesSetupDoc()
		{
			FieldNameMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		}
	}

    public enum IndexedPropertiesType
    {
        FieldMapping,
        Scripted
    }
}
