using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;
using RavenFS.Tests.Synchronization.IO;
using Xunit;

namespace RavenFS.Tests.Synchronization
{
	public class NeedListGeneratorTest
	{
		private static RandomlyModifiedStream GetSeedStream()
		{
			return new RandomlyModifiedStream(GetSourceStream(), 0.01, 1);
		}

		private static RandomStream GetSourceStream()
		{
			return new RandomStream(15*1024*1024, 1);
		}

		[MtaFact]
		public void ctor_and_dispose()
		{
			using (var signatureRepository = CreateSignatureRepositoryFor("test"))
			using (var tested = new NeedListGenerator(signatureRepository, signatureRepository))
			{
				Assert.NotNull(tested);
			}
		}

		[MtaFact]
		public void Generate_check()
		{
			IList<SignatureInfo> sourceSignatureInfos;
			IList<SignatureInfo> seedSignatureInfos;
			using (var sourceSignatureRepository = CreateSignatureRepositoryFor("test"))
			using (var seedSignatureRepository = CreateSignatureRepositoryFor("test"))
			{
				using (var generator = new SigGenerator())
				{
					seedSignatureInfos = generator.GenerateSignatures(GetSeedStream(), "test", seedSignatureRepository);
				}
				var sourceStream = GetSourceStream();
				using (var generator = new SigGenerator())
				{
					sourceSignatureInfos = generator.GenerateSignatures(sourceStream, "test", sourceSignatureRepository);
				}
				var sourceSize = sourceStream.Length;
				using (var tested = new NeedListGenerator(sourceSignatureRepository, seedSignatureRepository))
				{
					var result = tested.CreateNeedsList(seedSignatureInfos.Last(), sourceSignatureInfos.Last());
					Assert.NotNull(result);

					Assert.Equal(0, sourceSize - result.Sum(x => Convert.ToInt32(x.BlockLength)));
				}
			}
		}

		[MtaFact]
		public void Synchronize_file_with_different_beginning()
		{
			const int size = 5000;
			var differenceChunk = new MemoryStream();
			var sw = new StreamWriter(differenceChunk);

			sw.Write("Coconut is Stupid");
			sw.Flush();

			var sourceContent = PrepareSourceStream(size);
			sourceContent.Position = 0;
			var seedContent = new CombinedStream(differenceChunk, sourceContent);

			using (var sourceSignatureRepository = CreateSignatureRepositoryFor("test2"))
			using (var seedSignatureRepository = CreateSignatureRepositoryFor("test1"))
			{
				IList<SignatureInfo> seedSignatureInfos;
				using (var generator = new SigGenerator())
				{
					seedContent.Seek(0, SeekOrigin.Begin);
					seedSignatureInfos = generator.GenerateSignatures(seedContent, "test1", seedSignatureRepository);
				}
				IList<SignatureInfo> sourceSignatureInfos;
				using (var generator = new SigGenerator())
				{
					sourceContent.Seek(0, SeekOrigin.Begin);
					sourceSignatureInfos = generator.GenerateSignatures(sourceContent, "test2", sourceSignatureRepository);
				}
				var sourceSize = sourceContent.Length;

				using (var tested = new NeedListGenerator(seedSignatureRepository, sourceSignatureRepository))
				{
					var result = tested.CreateNeedsList(seedSignatureInfos.Last(), sourceSignatureInfos.Last());
					Assert.NotNull(result);
					Assert.Equal(2, result.Count);
					Assert.Equal(0, sourceSize - result.Sum(x => Convert.ToInt32(x.BlockLength)));
				}
			}
		}

		private static MemoryStream PrepareSourceStream(int lines)
		{
			var ms = new MemoryStream();
			var writer = new StreamWriter(ms);

			for (var i = 1; i <= lines; i++)
			{
				for (var j = 0; j < 100; j++)
				{
					writer.Write(i.ToString("D4"));
				}
				writer.Write("\n");
			}
			writer.Flush();

			return ms;
		}

		private static ISignatureRepository CreateSignatureRepositoryFor(string fileName)
		{
			return new VolatileSignatureRepository(fileName);
		}
	}
}