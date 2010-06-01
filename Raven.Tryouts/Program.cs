using System;
using System.Linq;
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
            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(8081);
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}