namespace Corax.Queries
{
    public interface ITermProvider
    { 
        void Reset();
        bool Next(out TermMatch term);
        QueryInspectionNode Inspect();

        string DebugView => Inspect().ToString();
    }    
}
