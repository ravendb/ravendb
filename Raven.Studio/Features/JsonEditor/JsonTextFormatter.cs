using System;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Text.Utility;

namespace Raven.Studio.Features.JsonEditor
{
    public class JsonTextFormatter : ITextFormatter
    {
        /// <summary>
        ///   Formats the specified snapshot range.
        /// </summary>
        /// <param name="snapshotRange"> The snapshot range. </param>
        public void Format(TextSnapshotRange snapshotRange)
        {
            // Get the snapshot
            var snapshot = snapshotRange.Snapshot;
            if (snapshot == null)
                return;

            // Get the snapshot reader
            var reader = snapshot.GetReader(snapshotRange.StartOffset);

            // Get the code document
            var document = snapshot.Document as ICodeDocument;
            if (document == null)
                return;

            // Get the tab size
            var tabSize = document.TabSize;

            // Create a text change object for the document
            var options = new TextChangeOptions();
            // Changes must occur sequentially so that we can use unmodified offsets while looping over the document
            options.OffsetDelta = TextChangeOffsetDelta.SequentialOnly;
            options.RetainSelection = true;
            var change = document.CreateTextChange(TextChangeTypes.AutoFormat, options);

            // Keep track of the last non whitespace token Id
            int lastNonWhitespaceTokenId = -1;

            // Keep track of the indent level
            int indentLevel = 0;

            // Loop through the document
            while ((reader.Token != null) && (reader.Offset < snapshotRange.EndOffset))
            {
                // If the token is whitespace, delete the text
                if (reader.Token.Id == JsonTokenId.Whitespace)
                {
                    change.DeleteText(reader.Token.TextRange);
                }
                else
                {
                    // The token is not whitespace

                    // Create a variable that will contain the text to be inserted
                    string insertText = "";

                    // Determine the insertText value based on the previous non-whitespace token and the current token
                    switch (lastNonWhitespaceTokenId)
                    {
                        case JsonTokenId.OpenCurlyBrace:
                        case JsonTokenId.OpenSquareBrace:
                            {
                                if (reader.Token.Id != JsonTokenId.CloseCurlyBrace &&
                                    reader.Token.Id != JsonTokenId.CloseSquareBrace)
                                {
                                    indentLevel++;
                                }
                                insertText = Environment.NewLine +
                                             StringHelper.GetIndentText(document.AutoConvertTabsToSpaces, tabSize,
                                                                        indentLevel * tabSize);
                                break;
                            }
                        case JsonTokenId.Colon :
                            {
                                insertText = " ";
                                break;
                            }
                        case JsonTokenId.Comma :
                            {
                                if (reader.Token.Id == JsonTokenId.CloseCurlyBrace ||
                                    reader.Token.Id == JsonTokenId.CloseSquareBrace)
                                {
                                    indentLevel = Math.Max(0, indentLevel - 1);
                                }
                                insertText = Environment.NewLine +
                                            StringHelper.GetIndentText(document.AutoConvertTabsToSpaces, tabSize,
                                                                       indentLevel * tabSize);
                                break;
                            }
                        case JsonTokenId.CloseCurlyBrace:
                        case JsonTokenId.CloseSquareBrace:
                        case JsonTokenId.False:
                        case JsonTokenId.True:
                        case JsonTokenId.Number:
                        case JsonTokenId.StringEndDelimiter:
                        case JsonTokenId.Null:
                            {
                                if (reader.Token.Id == JsonTokenId.CloseCurlyBrace ||
                                    reader.Token.Id == JsonTokenId.CloseSquareBrace)
                                {
                                    indentLevel = Math.Max(0, indentLevel - 1);
                                    insertText = Environment.NewLine +
                                                 StringHelper.GetIndentText(document.AutoConvertTabsToSpaces, tabSize,
                                                                            indentLevel*tabSize);
                                }
                            }
                            break;
                    }
                    // Insert the replacement text
                    change.InsertText(reader.Token.StartOffset, insertText);

                    // Update the last non-whitespace token Id
                    lastNonWhitespaceTokenId = reader.Token.Id;
                }
                // Go to the next token
                reader.GoToNextToken();
            }

            // If the entire document was formatted, add a newline to the end
            if ((snapshot.SnapshotRange.StartOffset == snapshotRange.StartOffset)
                && (snapshot.SnapshotRange.EndOffset == snapshotRange.EndOffset))
                change.InsertText(snapshotRange.EndOffset, "\n");

            // Apply the changes
            change.Apply();
        }
    }
}