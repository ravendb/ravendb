using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.TestBase
{
    public class Animal_Search : AbstractMultiMapIndexCreationTask
    {
        public Animal_Search()
        {
            AddMapForAll<Animal>(animals => from animal in animals
                select new {animal.Name});
        }
    }
}