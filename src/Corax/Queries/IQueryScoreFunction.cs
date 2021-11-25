namespace Corax.Queries
{
    public interface IQueryScoreFunction { }

    public struct NullScoreFunction : IQueryScoreFunction { }

    public struct ConstantScoreFunction : IQueryScoreFunction
    {
        public readonly float Value;

        public ConstantScoreFunction(float value)
        {
            Value = value;
        }
    }

    public struct PerFieldScoreFunction : IQueryScoreFunction
    {
    }

    public struct PerDocumentScoreFunction : IQueryScoreFunction
    {
    }
}
