using System;

#if !DNXCORE50
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
#if !DNXCORE50
            for (int i = 0; i < 100; i++)
            {
                using (var test = new FileSessionListenersTests())
                {
                    Console.WriteLine(i);

                    test.ConflictListeners_RemoteVersion().Wait();

        }
    }
#endif
}
    }
}
