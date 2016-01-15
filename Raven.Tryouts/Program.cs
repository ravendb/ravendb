using System;

#if !DNXCORE50
using Raven.Tests.Issues;
using Raven.Tests.MailingList;
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
#if !DNXCORE50
           new RavenDB_4161().CanUseTransfromer();
#endif
        }
    }
}
