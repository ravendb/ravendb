using System;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Raven.Client.Tests.Document;
using Raven.Database.Linq;

namespace RavenTestbed
{
    class Program
    {
        static void Main(string[] args)
        {
        	dynamic dynamicList = new DynamicJsonObject.DynamicList(new object[]{1,2});
			Console.WriteLine(dynamicList.Length);
        	foreach (var item in dynamicList.DefaultIfEmpty())
        	{
        		Console.WriteLine(item != null);
        	}
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}