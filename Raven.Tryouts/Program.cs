using System;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using System.IO;
using System.Diagnostics;
using Raven.Server;

namespace RavenTestbed
{
    class Program
    {
        static void Main(string[] args)
        {
			Console.WriteLine(JToken.FromObject(DateTime.Now).ToString(Formatting.Indented));
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}