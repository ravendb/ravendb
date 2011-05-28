namespace Raven.StackOverflow.Etl
{
    public interface ICommand
    {
        string CommandText { get; }
        void Run();
    }
}