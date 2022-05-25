using System.Runtime.CompilerServices;
using Corax.Queries;
using Corax.Utils.Spatial;
using Spatial4n.Core.Shapes;

namespace Corax.Utils;

public readonly struct OrderMetadata
{
    public readonly string FieldName;
    public readonly int FieldId;
    public readonly bool HasBoost;
    public readonly bool Ascending;
    public readonly MatchCompareFieldType FieldType;
    public readonly IPoint Point;
    public readonly double Round;
    public readonly SpatialUnits Units;

    public OrderMetadata(bool hasBoost, MatchCompareFieldType fieldType, bool ascending = true)
    {
        Unsafe.SkipInit(out FieldName);
        Unsafe.SkipInit(out FieldId);
        Unsafe.SkipInit(out Point);
        Unsafe.SkipInit(out Round);
        Unsafe.SkipInit(out Units);

        HasBoost = hasBoost;
        Ascending = ascending;
        FieldType = fieldType;
    }

    public OrderMetadata(string fieldName, int fieldId, bool ascending, MatchCompareFieldType fieldType)
    {
        Unsafe.SkipInit(out HasBoost);
        Unsafe.SkipInit(out Point);
        Unsafe.SkipInit(out Round);
        Unsafe.SkipInit(out Units);

        FieldName = fieldName;
        FieldId = fieldId;
        Ascending = ascending;
        FieldType = fieldType;
    }

    public OrderMetadata(string fieldName, int fieldId, bool ascending, MatchCompareFieldType fieldType, IPoint point, double round, SpatialUnits units)
    {
        Unsafe.SkipInit(out HasBoost);
        FieldName = fieldName;
        FieldId = fieldId;
        Ascending = ascending;
        FieldType = fieldType;
        Round = round;
        Point = point;
        Units = units;
    }
}
