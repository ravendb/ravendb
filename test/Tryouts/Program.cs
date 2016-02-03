using System;
using System.Diagnostics;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using BlittableTests.Documents;
using Raven.Server.Config.Categories;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Tests.Core;
using Sparrow;
using Voron;
using Voron.Tests.Bugs;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string Name { get; set; }
            public int Age { get; set; }

            public User()
            {
                Age = 33;
            }
        }
        public unsafe static void Main(string[] args)
        {
            using (var c = new Crud())
            {
                c.CanSaveAndLoad().Wait();
            }
        }
    }
}