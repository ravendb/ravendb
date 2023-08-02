namespace Sparrow.Utils
{
    public sealed class StackNode<T>
    {
        public T Value;
        public StackNode<T> Next;
    }
}
