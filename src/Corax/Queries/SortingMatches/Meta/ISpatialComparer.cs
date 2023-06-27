using Corax.Utils.Spatial;
using Spatial4n.Shapes;

namespace Corax.Queries.SortingMatches.Meta;

public interface ISpatialComparer : IMatchComparer
{
    double Round { get; }
        
    SpatialUnits Units { get; }
        
    IPoint Point { get; }
}
