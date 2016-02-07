using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Server.Documents;
using Raven.Tests.Core;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var x = new Crud())
            {
                x.CanSaveAndLoad().Wait();
            }
        }
    }
}
