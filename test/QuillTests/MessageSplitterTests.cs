using FastTests;
using Raven.Quill.Hosting;
using Raven.Quill.Channels;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MessageSplitterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Short_text_is_returned_unchanged()
    {
        var parts = MessageSplitter.Split("hello world", TelegramOptions.ApiMessageLimit);

        Assert.Equal(["hello world"], parts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Text_at_exactly_the_limit_is_not_split()
    {
        var text = new string('a', 4096);

        Assert.Equal([text], MessageSplitter.Split(text, TelegramOptions.ApiMessageLimit));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Long_text_splits_at_the_last_sentence_boundary()
    {
        var first = new string('a', 20) + ".";
        var second = new string('b', 20);
        var parts = MessageSplitter.Split(first + " " + second, limit: 30);

        Assert.Equal([first, second], parts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Newlines_count_as_sentence_boundaries()
    {
        var first = new string('a', 20);
        var second = new string('b', 20);
        var parts = MessageSplitter.Split(first + "\n" + second, limit: 30);

        Assert.Equal([first, second], parts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Text_without_boundaries_hard_splits_at_the_limit()
    {
        var text = new string('a', 70);
        var parts = MessageSplitter.Split(text, limit: 30);

        Assert.Equal([new string('a', 30), new string('a', 30), new string('a', 10)], parts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Hard_split_never_tears_a_surrogate_pair()
    {
        var text = string.Concat(Enumerable.Repeat("\U0001F600", 20));
        var parts = MessageSplitter.Split(text, limit: 15);

        Assert.All(parts, part => Assert.True(part.Length <= 15));
        Assert.All(parts, part => Assert.False(char.IsHighSurrogate(part[^1])));
        Assert.All(parts, part => Assert.False(char.IsLowSurrogate(part[0])));
        Assert.Equal(text, string.Concat(parts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Every_part_stays_within_the_limit()
    {
        var text = string.Join(" ", Enumerable.Range(0, 300).Select(i => $"Sentence number {i} ends here."));
        var parts = MessageSplitter.Split(text, TelegramOptions.ApiMessageLimit);

        Assert.True(parts.Count > 1);
        Assert.All(parts, part => Assert.True(part.Length <= 4096));
        Assert.All(parts, part => Assert.False(string.IsNullOrWhiteSpace(part)));
    }
}
