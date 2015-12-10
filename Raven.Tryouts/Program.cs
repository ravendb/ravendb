using System;

using Raven.Tests.FileSystem.ClientApi;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                using (var test = new FileSessionListenersTests())
                {
                    Console.WriteLine(i);

                    test.ConflictListeners_RemoteVersion().Wait();

                }
            }
        }
    }
}
