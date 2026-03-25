namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public interface ICoraxClause
{
    public float? Boosting { get; set; }
}
