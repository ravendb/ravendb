using System;
using System.IO;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Server.Documents;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27817 : RavenTestBase
{
    public RavenDB_27817(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Attachments)]
    [InlineData("ẞ\"")]
    [InlineData("ß\"")]
    [InlineData("ss\"")]
    [InlineData("ẞ")]
    [InlineData("aẞẞb\"c\\d")]
    [InlineData("ẞ\"Ⱥ")] // lowercasing shrinks one char and grows another, so the total UTF-8 length is unchanged
    public void CanLoadDocumentWithAttachmentNameWhoseLowercaseHasDifferentUtf8Length(string attachmentName)
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        using (var content = new MemoryStream(new byte[] { 120 }))
        {
            session.Store(new { Value = "test" }, "docs/1");
            session.Advanced.Attachments.Store("docs/1", attachmentName, content, "text/plain");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var document = session.Load<object>("docs/1");
            Assert.NotNull(document);
            Assert.Equal(attachmentName, Assert.Single(session.Advanced.Attachments.GetNames(document)).Name);
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData("docs/ẞ\"")]
    [InlineData("docs/aẞẞb\"c\\d")]
    public void CanLoadDocumentWhoseIdLowercaseHasDifferentUtf8Length(string id)
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            session.Store(new { Value = "test" }, id);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var document = session.Load<object>(id);
            Assert.NotNull(document);
            Assert.Equal(id, session.Advanced.GetDocumentId(document));
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [InlineData("ẞ\"", new[] { 3 })]
    [InlineData("ẞ\"Ⱥ", new[] { 3 })]
    [InlineData("aẞẞb\"c\\d", new[] { 8, 1 })]
    // long enough to reserve a 2-byte length prefix while the actual ID needs only 1 byte
    [InlineData("é\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", new[] { 2 })]
    [InlineData("ẞ\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", new[] { 3 })]
    public void StorageKeyEscapePositionsPointToTheActualIdBytes(string id, int[] expectedEscapePositions)
    {
        AssertStorageKey(id, Encoding.UTF8.GetBytes(id), expectedEscapePositions);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void StorageKeyLengthIsCorrectWhenControlCharsEscapingCrossesVarIntLengthBoundary()
    {
        // 28 UTF-8 bytes (1-byte length prefix) become 153 bytes (2-byte length prefix) once each \u0001 is expanded in place
        var id = "ẞ" + new string('\u0001', 25);
        var expectedId = Encoding.UTF8.GetBytes("ẞ" + string.Concat(Enumerable.Repeat("\\u0001", 25)));

        AssertStorageKey(id, expectedId, []);
    }

    private static unsafe void AssertStorageKey(string id, byte[] expectedId, int[] expectedEscapePositions)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using (DocumentIdWorker.Compatibility.GetLowerIdSliceAndStorageKey(allocator, id, out _, out Slice idSlice))
        {
            byte* ptr = idSlice.Content.Ptr;
            byte* end = ptr + idSlice.Size;

            var idSize = ReadVariableSizeInt(ref ptr);
            Assert.Equal(expectedId, new ReadOnlySpan<byte>(ptr, idSize).ToArray());
            ptr += idSize;

            var escapePositions = new int[ReadVariableSizeInt(ref ptr)];
            for (var i = 0; i < escapePositions.Length; i++)
                escapePositions[i] = ReadVariableSizeInt(ref ptr);

            Assert.Equal(expectedEscapePositions, escapePositions);
            Assert.True(ptr == end, "The storage key has trailing bytes");
        }
    }

    private static unsafe int ReadVariableSizeInt(ref byte* ptr)
    {
        int value = 0, shift = 0;
        byte b;
        do
        {
            b = *ptr++;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        return value;
    }
}
