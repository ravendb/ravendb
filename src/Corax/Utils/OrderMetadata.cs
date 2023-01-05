using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Queries;
using Corax.Utils.Spatial;
using Spatial4n.Shapes;

namespace Corax.Utils;

public readonly struct OrderMetadata
{
    public readonly FieldMetadata Field;
    public readonly bool HasBoost;
    public readonly bool Ascending;
    public readonly MatchCompareFieldType FieldType;
    public readonly IPoint Point;
    public readonly double Round;
    public readonly SpatialUnits Units;

    public OrderMetadata(bool hasBoost, MatchCompareFieldType fieldType, bool ascending = true)
    {
        Unsafe.SkipInit(out Field);
        Unsafe.SkipInit(out Point);
        Unsafe.SkipInit(out Round);
        Unsafe.SkipInit(out Units);

        HasBoost = hasBoost;
        Ascending = ascending;
        FieldType = fieldType;
    }

    public OrderMetadata(FieldMetadata field, bool ascending, MatchCompareFieldType fieldType)
    {
        Unsafe.SkipInit(out HasBoost);
        Unsafe.SkipInit(out Point);
        Unsafe.SkipInit(out Round);
        Unsafe.SkipInit(out Units);

        Field = field;
        Ascending = ascending;
        FieldType = fieldType;
    }

    public OrderMetadata(FieldMetadata field, bool ascending, MatchCompareFieldType fieldType, IPoint point, double round, SpatialUnits units)
    {
        Unsafe.SkipInit(out HasBoost);
        Field = field;
        Ascending = ascending;
        FieldType = fieldType;
        Round = round;
        Point = point;
        Units = units;
    }
}
