using System;
using Newtonsoft.Json.Linq;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using System.Linq;

namespace Raven.Tryouts
{
	internal class Program
	{
		private const string query =
			@"
    from doc in docs
	where doc[""@meta""][""@type""] == ""Posts""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
";

		public static void Main()
		{
			try
			{
				var abstractViewGenerator = new DynamicViewCompiler("a", new IndexDefinition{Map = query}).GenerateInstance();
				var objects = abstractViewGenerator.MapDefinition(new[] {new DynamicJsonObject(JObject.Parse("{'@meta': {'@type': 'Posts'}}"))}).ToArray<object>();
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}
}