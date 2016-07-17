using System;
using System.IO;
using Raven.Abstractions.Indexing;
using System.Linq;
using System.Text;
using Raven.Server.Documents.Transformers;
using Xunit;

namespace FastTests.Server.Documents.Transformers
{
    public class BasicTransformers : RavenLowLevelTestBase
    {
        [Fact]
        public void CanPersist()
        {
            var path = NewDataPath();
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var transformerId1 = database.TransformerStore.CreateTransformer(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Name })",
                    LockMode = TransformerLockMode.LockedIgnore,
                    Temporary = true,
                    Name = "Transformer1"
                });

                Assert.Equal(1, transformerId1);

                var transformerId2 = database.TransformerStore.CreateTransformer(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Email })",
                    LockMode = TransformerLockMode.Unlock,
                    Temporary = false,
                    Name = "Transformer2"
                });

                Assert.Equal(2, transformerId2);
            }

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path, modifyConfiguration: configuration => configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = true))
            {
                var transformers = database
                    .TransformerStore
                    .GetTransformers()
                    .OrderBy(x => x.TransformerId)
                    .ToList();

                Assert.Equal(2, transformers.Count);

                var transformer = transformers[0];
                Assert.Equal("Transformer1", transformer.Name);
                Assert.Equal("Transformer1", transformer.Definition.Name);
                Assert.Equal(1, transformer.TransformerId);
                Assert.Equal(1, transformer.Definition.TransfomerId);
                Assert.Equal("results.Select(x => new { Name = x.Name })", transformer.Definition.TransformResults);
                Assert.Equal(TransformerLockMode.LockedIgnore, transformer.Definition.LockMode);
                Assert.True(transformer.Definition.Temporary);

                transformer = transformers[1];
                Assert.Equal("Transformer2", transformer.Name);
                Assert.Equal("Transformer2", transformer.Definition.Name);
                Assert.Equal(2, transformer.TransformerId);
                Assert.Equal(2, transformer.Definition.TransfomerId);
                Assert.Equal("results.Select(x => new { Name = x.Email })", transformer.Definition.TransformResults);
                Assert.Equal(TransformerLockMode.Unlock, transformer.Definition.LockMode);
                Assert.False(transformer.Definition.Temporary);
            }
        }

        [Fact]
        public void WillLoadAsFaulty()
        {
            var path = NewDataPath();
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var transformerId1 = database.TransformerStore.CreateTransformer(new TransformerDefinition
                {
                    TransformResults = "results.Select(x => new { Name = x.Name })",
                    LockMode = TransformerLockMode.LockedIgnore,
                    Temporary = true,
                    Name = "Transformer1"
                });

                Assert.Equal(1, transformerId1);
            }

            var encodedName = Convert.ToBase64String(Encoding.UTF8.GetBytes("Transformer1"));
            var transformerFilePath = Path.Combine(path, "Indexes", "Transformers", $"1.{encodedName}{Transformer.FileExtension}");
            Assert.True(File.Exists(transformerFilePath));

            File.WriteAllText(transformerFilePath, string.Empty);

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path, modifyConfiguration: configuration => configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = false))
            {
                var transformers = database
                    .TransformerStore
                    .GetTransformers()
                    .OrderBy(x => x.TransformerId)
                    .ToList();

                Assert.Equal(1, transformers.Count);

                var transformer = transformers[0];
                Assert.Equal("Transformer1", transformer.Name);
                Assert.Equal(1, transformer.TransformerId);

                var e = Assert.Throws<NotSupportedException>(() => transformer.SetLock(TransformerLockMode.LockedIgnore));
                Assert.Equal("Transformer with id 1 is in-memory implementation of a faulty transformer", e.Message);
            }
        }
    }
}