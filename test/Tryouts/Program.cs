using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Voron;
using FastTests.Voron.Streams;
using SlowTests.Voron;
using StressTests;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            using (var a = new SlowTests.MailingList.RenamedProperty())
            {
                a.OrderByWithAttributeShouldStillWork();
            }
        }
    }


}

