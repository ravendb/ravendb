using System;
using Raven.Database.Indexing;
using Raven.Client.Tests.Document;

namespace RavenTestbed
{
    class Program
    {
        static void Main(string[] args)
        {
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}