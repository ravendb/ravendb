using System;
using System.IO;
using FastTests.Voron.Bugs;
using Voron;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var a = new RavenDB_6350())
            {
                a.AsyncCommitShouldNotCorruptState();
            }
        }
    }
}