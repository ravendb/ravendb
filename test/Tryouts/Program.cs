using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Blittable.BlittableJsonWriterTests;
using FastTests.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Tests.Core;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var c = new Crud())
            {
                c.CanSaveAndLoad().Wait();
            }
        }
    }
}
