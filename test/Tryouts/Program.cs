using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using System.Threading.Tasks;
using FastTests.Server.Documents.SqlReplication;
using Raven.Abstractions.Data;
using Raven.Client.Document;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string[] Tags { get; set; }
        }

        public static event EventHandler Foo;

        public static void Main(string[] args)
        {
            new CanReplicate().SimpleTransformation().Wait();
        }

        private static void Program_Foo(object sender, EventArgs e)
        {
            //throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void Program_Foo2(object sender, EventArgs e)
        {
            //throw new NotImplementedException();
        }
    }
}