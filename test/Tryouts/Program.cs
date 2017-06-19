using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using FastTests.Client.Subscriptions;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var a = new CriteriaScript())
            {
                a.CriteriaScriptWithTransformation(false).Wait();
            }
        }
    }
}
