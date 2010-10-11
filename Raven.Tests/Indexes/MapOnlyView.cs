using System.ComponentModel;
using Raven.Database.Linq;

namespace Raven.Tests.Indexes
{
    [DisplayName("Compiled/View")]
    public class MapOnlyView : AbstractViewGenerator
    {
        public MapOnlyView()
        {
            MapDefinition = source => from doc in source
                                      select doc;
        }
    }
}