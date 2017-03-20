namespace SlowTests
{
    public class Program
    {
        public static int Main(string[] args)
        {
            return Xunit.Runner.DotNet.Program.Main(new[] { $"{typeof(Program).Namespace}.dll" });
        }
    }
}