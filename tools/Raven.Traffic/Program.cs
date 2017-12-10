using System;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;

namespace Raven.Traffic
{
    internal static class Program
    {
        private static int Main(string[] args)
        {
            return CommandLineApp.Run(args);
        }
    }
}
