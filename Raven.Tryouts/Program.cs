using System;

using Raven.Tests.FileSystem.ClientApi;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main()
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.Clear();
                Console.WriteLine(i);
                using (var x = new FileSessionListenersTests())
                {
                    x.ConflictListeners_RemoteVersion().Wait();
                }
            }
        }
    }
}
