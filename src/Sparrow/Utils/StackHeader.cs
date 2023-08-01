namespace Sparrow.Utils
{
    public class StackHeader<T>
    {
        public static readonly StackNode<T> HeaderDisposed = new StackNode<T>();

        public StackNode<T> Head;
    }
}
