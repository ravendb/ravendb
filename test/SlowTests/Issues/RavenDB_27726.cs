using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27726 : RavenTestBase
    {
        public RavenDB_27726(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanReadLastCharacterOfLongNonAsciiStringInJavaScript()
        {
            var text = string.Concat(Enumerable.Repeat("Hörübung für Kinder, Übungsgröße ", 10));

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Note { Text = text }, "notes/1");
                session.SaveChanges();
            }

            store.Operations.Send(new PatchOperation("notes/1", changeVector: null, new PatchRequest
            {
                Script = "this.Length = this.Text.length; this.Last = this.Text.charAt(this.Text.length - 1); this.EndsWith = this.Text.endsWith('öße ');"
            }));

            using (var session = store.OpenSession())
            {
                var note = session.Load<Note>("notes/1");

                Assert.Equal(text.Length, note.Length);
                Assert.Equal(text[^1].ToString(), note.Last);
                Assert.True(note.EndsWith);
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
        public void JavaScriptIndexSeesCharacterCountOfLongNonAsciiString()
        {
            var text = string.Concat(Enumerable.Repeat("Hörübung für Kinder, Übungsgröße ", 10));

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Note { Text = text }, "notes/1");
                session.SaveChanges();
            }

            new Notes_ByTextLength().Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var result = session.Advanced
                    .RawQuery<TextLengthResult>("from index 'Notes/ByTextLength' select TextLength")
                    .Single();

                Assert.Equal(text.Length, result.TextLength);
            }
        }

        private class Notes_ByTextLength : AbstractJavaScriptIndexCreationTask
        {
            public Notes_ByTextLength()
            {
                Maps = new HashSet<string> { "map('Notes', function (n) { return { TextLength: n.Text.length }; })" };
                Fields = new Dictionary<string, IndexFieldOptions> { ["TextLength"] = new IndexFieldOptions { Storage = FieldStorage.Yes } };
            }
        }

        private class TextLengthResult
        {
            public int TextLength { get; set; }
        }

        private class Note
        {
            public string Text { get; set; }
            public int Length { get; set; }
            public string Last { get; set; }
            public bool EndsWith { get; set; }
        }
    }
}
