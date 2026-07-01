namespace Corax.Querying.Planning;

public sealed class InspectionOp
{
    public string Name;
    public string Dispatch;
    public string FieldName;
    public string ClauseType;
    public bool IsNegated;

    public int FlatClauseIndex = -1;

    // The _original_ index of the operation, before execution time filtering
    public int OpIndex = -1;

    // Bitmap slot to write to, -1 is an invalid value for a slot
    public int DestSlot = -1;

    // For AND / OR / AND NOT - the source bitmap 
    public int SourceSlot = -1;

    // This operation checks if there are few enough entries to can to entry scan mode.
    public bool IsEntryScanGate;

    // The number of items in an IN / ALL IN clause
    public int RangeCountIndex = -1;
}
