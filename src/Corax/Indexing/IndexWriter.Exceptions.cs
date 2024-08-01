using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow;
using Voron;

namespace Corax.Indexing;

public partial class IndexWriter
{
    private static void ThrowTriedToDeleteTermThatDoesNotExists(Slice term, IndexedField indexedField)
    {
        throw new InvalidOperationException(
            $"Attempt to remove term: '{term}' for field {indexedField.Name}, but it does not exists! This is a bug.");
    }
    
    private static void ThrowMoreThanOneRemovalFoundForSingleItem(long idInTree, in EntriesModifications entries, long existingEntryId, short existingFrequency)
    {
        throw new InvalidOperationException($"More than one removal found for a single item, which is impossible. " +
                                            $"{Environment.NewLine}Current tree id: {idInTree}" +
                                            $"{Environment.NewLine}Current entry id {existingEntryId}" +
                                            $"{Environment.NewLine}Current term frequency: {existingFrequency}" +
                                            $"{Environment.NewLine}Items we wanted to delete (entryId|Frequency): " +
                                            $"{string.Join(", ", entries.Removals.ToSpan().ToArray().Select(i => $"({i.EntryId}|{i.Frequency})"))}");
    }
    
    [DoesNotReturn]
    private void ThrowInvalidTokenFoundOnBuffer(IndexedField field, ReadOnlySpan<byte> value, Span<byte> wordsBuffer, Span<Token> tokens, Token token)
    {
        throw new InvalidDataException(
            $"{Environment.NewLine}Got token with: " +
            $"{Environment.NewLine}\tOFFSET {token.Offset}" +
            $"{Environment.NewLine}\tLENGTH: {token.Length}." +
            $"{Environment.NewLine}Total amount of tokens: {tokens.Length}" +
            $"{Environment.NewLine}Buffer contains '{Encodings.Utf8.GetString(wordsBuffer)}' and total length is {wordsBuffer.Length}" +
            $"{Environment.NewLine}Buffer from ArrayPool: {Environment.NewLine}\tbyte buffer is {_encodingBufferHandler.Length} {Environment.NewLine}\ttokens buffer is {_tokensBufferHandler.Length}" +
            $"{Environment.NewLine}Original span contains '{Encodings.Utf8.GetString(value)}' with total length {value.Length}" +
            $"{Environment.NewLine}Field " +
            $"{Environment.NewLine}\tid: {field.Id}" +
            $"{Environment.NewLine}\tname: {field.Name}");
    }
    
    private static void ThrowInconsistentDynamicFieldCreation(IndexFieldBinding binding, FieldIndexingMode originalIndexingMode)
    {
        throw new InvalidDataException(
            $"Inconsistent dynamic field creation options were detected. Field '{binding.FieldName}' was created with '{originalIndexingMode}' analyzer but now '{binding.FieldIndexingMode}' analyzer was specified. This is not supported");
    }
    
    private static void ThrowPreviousBuilderIsNotDisposed()
    {
        throw new NotSupportedException("You *must* dispose the previous builder before calling it again");
    }
    
    private static void ThrowUnableToLocateEntry(long entryToDelete)
    {
        throw new InvalidOperationException("Unable to locate entry id: " + entryToDelete);
    }
}
