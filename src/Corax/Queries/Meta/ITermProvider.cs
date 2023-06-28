namespace Corax.Queries
{
    public interface ITermProvider
    {
        bool IsOrdered { get; }
        void Reset();
        bool Next(out TermMatch term);
        QueryInspectionNode Inspect();

        string DebugView => Inspect().ToString();
    }    
}
