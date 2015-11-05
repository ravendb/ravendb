using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.TestBase
{
    public class Animals_Search : AbstractMultiMapIndexCreationTask
    {
        public Animals_Search()
        {
            AddMapForAll<Animal>(animals => from animal in animals
                select new {animal.Name});
        }
    }
}
