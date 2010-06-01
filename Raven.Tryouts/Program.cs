using System;
using Raven.Database.Indexing;

namespace RavenTestbed
{
    class Program
    {
        static void Main(string[] args)
        {
			Console.WriteLine(NumberUtil.NumberToString(decimal.MinValue));
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}