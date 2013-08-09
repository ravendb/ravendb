using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq.Expressions;
using System.Runtime;
using Nevar.Debugging;
using Nevar.Impl;
using Nevar.Tests.Storage;
using Nevar.Tests.Trees;

namespace Nevar.Tryout
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            new MultiTransactions().ShouldWork();
        }
    }
}