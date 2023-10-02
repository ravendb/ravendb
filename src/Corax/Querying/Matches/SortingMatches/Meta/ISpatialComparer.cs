using Corax.Utils.Spatial;
using Spatial4n.Shapes;

namespace Corax.Querying.Matches.SortingMatches.Meta;

public interface ISpatialComparer : IMatchComparer
{
    double Round { get; }
        
    SpatialUnits Units { get; }
        
    IPoint Point { get; }
}
