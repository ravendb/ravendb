using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.DocumentsCompression;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Voron.Data.Tables;
using Xunit.Abstractions;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Entities;
using Assert = Xunit.Assert;

namespace SlowTests.Voron.Issues;

public class RavenDB_21107 : RavenTestBase
{
    public RavenDB_21107(ITestOutputHelper output) : base(output)
    {

    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Compression)]
    [RavenData(Data = new object[]{ "DictionaryA" })]
    [RavenData(Data = new object[]{ "DictionaryB" })]
    [RavenData(Data = new object[]{ "Both" })]
    public async Task Recovery_Compression_Dictionaries_Should_Be_Consistent_Even_If_Previous_Files_Corrupted(Options options, string scenarioName)
    {
        var path = NewDataPath();
        using (var store = GetDocumentStore(new Options { Path = path, RunInMemory = false }))
        {
            await store.Maintenance.SendAsync(
                new UpdateDocumentsCompressionConfigurationOperation(new DocumentsCompressionConfiguration(compressRevisions: true, compressAllCollections: true)));

            await store.Maintenance.SendAsync(new CreateSampleDataOperation());
            Indexes.WaitForIndexing(store);

            await AssertCompressionRecoveryFiles(store, path, expectedNumberOfDictionaries: 2);

            // Corrupt files based on the provided scenario
            switch (scenarioName)
            {
                case "DictionaryA":
                    await CorruptFile("DictionaryA");
                    break;

                case "DictionaryB":
                    await CorruptFile("DictionaryB");
                    break;

                case "Both":
                    await CorruptFile("DictionaryA");
                    await CorruptFile("DictionaryB");
                    break;

                default:
                    Assert.Fail("Unexpected scenario");
                    break;
            }

            await using (var bulk = store.BulkInsert())
            {
                const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

                for (int i = 0; i < 1024; i++)
                {
                    await bulk.StoreAsync(new User { Name = chars });
                }
            }

            await AssertCompressionRecoveryFiles(store, path, expectedNumberOfDictionaries: 3);
        }

        return;

        async Task CorruptFile(string fileName)
        {
            string fileToCorrupt = $"{fileName}{TableValueCompressor.CompressionRecoveryExtension}";
            var firstFilePath = Path.Combine(path, fileToCorrupt);
            Assert.True(File.Exists(firstFilePath), $"The file {fileToCorrupt} does not exist.");

            byte[] originalFileContent = await File.ReadAllBytesAsync(firstFilePath);
            Assert.True(originalFileContent.Length > 100, $"The size of '{fileToCorrupt}' must be greater than 100 bytes for the test.");

            byte[] modifiedFileContent = new byte[originalFileContent.Length - 100];
            Array.Copy(originalFileContent, 100, modifiedFileContent, 0, originalFileContent.Length - 100);
            await File.WriteAllBytesAsync(firstFilePath, modifiedFileContent);
        }
    }

    private async Task AssertCompressionRecoveryFiles(DocumentStore store, string path, int expectedNumberOfDictionaries)
    {
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        int inStorageDictionariesCount;
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.Environment.ReadTransaction())
        {
            inStorageDictionariesCount = context.Environment.CompressionDictionariesHolder.GetInStorageDictionaries(tx).Count();
        }

        Dictionary<string, string> fileHashes = new();

        for (int i = 0; i < 2; i++)
        {
            string fileName = $"Dictionary{(i == 0 ? "A" : "B")}{TableValueCompressor.CompressionRecoveryExtension}";
            var pathToDic = Path.Combine(path, fileName);
            Assert.True(File.Exists(pathToDic),$"The file {fileName} was not found at the specified path '{pathToDic}'.");

            await using (var finalFileStream = File.Open(pathToDic, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var zip = new ZipArchive(finalFileStream))
            {
                Assert.True(inStorageDictionariesCount == expectedNumberOfDictionaries,
                    userMessage: $"The number of 'inStorageDictionaries' ({inStorageDictionariesCount}) " +
                                 $"is not equal to the expected number of dictionaries ({expectedNumberOfDictionaries}). " +
                                 $"Error occurred in the recovery file '{fileName}'.");

                Assert.True(inStorageDictionariesCount == zip.Entries.Count,
                    userMessage: $"The count of 'inStorageDictionaries' ({inStorageDictionariesCount}) " +
                                 $"is not equal to the count of 'zip.Entries' ({zip.Entries.Count}). " +
                                 $"Error occurred in the recovery file '{fileName}'.");

                for (int index = 0; index < zip.Entries.Count; index++)
                {
                    var entry = zip.Entries[index];

                    await using (var entryStream = entry.Open())
                    {
                        var hash = SHA256.Create();
                        byte[] fileHash = await hash.ComputeHashAsync(entryStream);
                        string fileHashString = BitConverter.ToString(fileHash).Replace("-", "").ToLowerInvariant();

                        if (i == 0)
                        {
                            fileHashes[entry.Name] = fileHashString;
                        }
                        else
                        {
                            if (fileHashes.TryGetValue(entry.Name, out var firstIterationHash))
                            {
                                Assert.True(firstIterationHash == fileHashString, userMessage:
                                    $"The hash of the file '{entry.Name}' doesn't match between '{fileName}' and 'DictionaryA{TableValueCompressor.CompressionRecoveryExtension}'. " +
                                    $"Hash from '{fileName}': {firstIterationHash}, " +
                                    $"Hash from 'DictionaryA{TableValueCompressor.CompressionRecoveryExtension}': {fileHashString}");
                            }
                            else
                            {
                                Assert.Fail($"The file '{entry.Name}' was found in '{fileName}', " +
                                            $"but it was not found in 'DictionaryA{TableValueCompressor.CompressionRecoveryExtension}'.");
                            }
                        }
                    }

                    var entryNameAsInt = int.Parse(Path.GetFileNameWithoutExtension(entry.Name));
                    var expectedIndex = index + 1;
                    Assert.True(entryNameAsInt == expectedIndex, userMessage:
                        $"In '{fileName}', the entry '{entry.Name}' has an incorrect index. " +
                        $"Expected index: {expectedIndex}, Actual index: {entryNameAsInt}. " +
                        $"Entries in the zip archive should have consecutive numbering starting from 1.");
                }
            }
        }
    }
}
