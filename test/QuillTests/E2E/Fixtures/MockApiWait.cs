namespace QuillTests.E2E.Fixtures;

internal static class MockApiWait
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(25);

    internal static Task UntilAsync(string mock, Func<bool> condition, string what, TimeSpan? timeout) =>
        UntilAsync(mock, () => Task.FromResult(condition()), what, timeout);

    internal static async Task UntilAsync(string mock, Func<Task<bool>> condition, string what, TimeSpan? timeout)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        while (await condition() == false)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"{mock}: timed out waiting for {what}");

            await Task.Delay(PollInterval);
        }
    }
}
