namespace Sparrow.Utils
{
    public class StackHeader<T>
    {
        public readonly static StackNode<T> HeaderDisposed = new StackNode<T>();

        public StackNode<T> Head;
    }
}
