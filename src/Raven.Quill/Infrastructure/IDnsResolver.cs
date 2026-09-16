namespace Raven.Quill.Infrastructure;

public interface IDnsResolver
{
    Task<string[]> ResolveIPv4Async(string hostname, CancellationToken token);
}
