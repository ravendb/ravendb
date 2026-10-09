namespace Raven.Quill.Hosting;

internal static class EnvironmentVariables
{
    public static void Read(string name, Action<string> apply)
    {
        var v = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(v) == false)
            apply(v);
    }
}
