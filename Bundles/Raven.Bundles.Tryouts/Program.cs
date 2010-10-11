using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Bundles.Tests.Expiration;

namespace Raven.Bundles.Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 10000; i++)
            {
                Console.Write(i+"\r");
                using (var x = new Expiration())
                {
                    x.After_expiry_passed_document_will_be_physically_deleted();
                }
            }
        }
    }
}
