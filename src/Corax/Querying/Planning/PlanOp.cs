namespace Corax.Querying.Planning;

public struct PlanOp
{
    public PlanOpKind Kind;
    public int ParamIndex;
    public int ParamIndex2;
    public int BitmapLocal;

    /// <summary>When true, suppress the empty-check early exit after an AND. Used for AND inside an OR, where we can't just abort</summary>
    public bool SkipEarlyExit;

    /// <summary>Build-time-only human label for the clause this op reads (e.g. "Name [Equals]").
    /// Null for pure bitmap-algebra / control-flow ops. Surfaced as a comment in the generated
    /// C# mirror; never read at execution time.</summary>
    public string DebugLabel;
}
