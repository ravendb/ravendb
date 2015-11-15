using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.MailingList;

namespace Raven.Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
            using(var x = new MbsCountIndex())
            {
                x.TypeIssue();
            }
        }
    }
}
