using System;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Raven.Client.Tests.Document;

namespace RavenTestbed
{
    class Program
    {
        static void Main(string[] args)
        {
        	new DocumentStore
        	{
        		Url = "http://localhost:8080"
        	}.Initialize();
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}