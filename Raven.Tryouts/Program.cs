using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Tryouts
{
	internal class Program
	{
	

		public static void Main()
		{
			var serializeObject = JsonConvert.SerializeObject(new IndexDefinition
			{
				Map = "abc",
				Reduce = "def",
				Stores = new Dictionary<string, FieldStorage>
				{
					{"ee", FieldStorage.Compress}
				}
			},Formatting.Indented,new JsonEnumConverter());
			Console.WriteLine(serializeObject);
			var definition = JsonConvert.DeserializeObject<IndexDefinition>(serializeObject, new JsonEnumConverter());
			Console.WriteLine(definition.Stores["ee"]);
		}
	}

}