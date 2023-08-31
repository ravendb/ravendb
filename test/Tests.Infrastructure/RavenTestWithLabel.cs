namespace Tests.Infrastructure
{
    public class RavenTestWithLabel<T>
    {
        public readonly T Data;
        public readonly string Label;

        public RavenTestWithLabel(T data, string label)
        {
            Data = data;
            Label = label;
        }
        public override string ToString() => Label;
    }

    public static class WithLabels
    {
        public static RavenTestWithLabel<T> Labeled<T>(this T source, string label) => new RavenTestWithLabel<T>(source, label);
    }
}
