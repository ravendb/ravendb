using System;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var x = new SlowTests.Voron.Full())
            {
                x.CanBackupAndRestore();
            }
        }
    }
}

