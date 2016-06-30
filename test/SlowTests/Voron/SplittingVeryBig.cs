using System;
using System.IO;
using System.Linq;
using Voron;
using Xunit;

namespace SlowTests.Voron
{
    public class SplittingVeryBig : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 10 * options.PageSize;
		}

		[Fact]
		public void ShouldBeAbleToWriteValuesGreaterThanLogAndReadThem()
		{
			var random = new Random(1234);
			var buffer = new byte[1024 * 512];
			random.NextBytes(buffer);

			using (var tx = Env.WriteTransaction())
			{
				tx.CreateTree("tree");
				tx.Commit();
			}

			using (var tx = Env.WriteTransaction())
			{
				tx.CreateTree("tree").Add("key1", new MemoryStream(buffer));
				tx.Commit();
			}

			using (var tx = Env.ReadTransaction())
			{
			    var read = tx.CreateTree("tree").Read("key1");
			    Assert.NotNull(read);

			    var reader = read.Reader;
			    Assert.Equal(buffer.Length, read.Reader.Length);
			    var bytes = reader.ReadBytes(read.Reader.Length);
			    Assert.Equal(buffer, bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray());
			}
		}

		[Fact]
		public void ShouldBeAbleToWriteValuesGreaterThanLogAndRecoverThem()
		{
			var random = new Random(1234);
			var buffer = new byte[1024 * 512];
			random.NextBytes(buffer);

			var options = StorageEnvironmentOptions.ForPath(DataDir);
			options.MaxLogFileSize = 10 * options.PageSize;

			using (var env = new StorageEnvironment(options, NullLoggerSetup))
			{
				using (var tx = env.WriteTransaction())
				{
					tx.CreateTree("tree");
					tx.Commit();
				}

				using (var tx = env.WriteTransaction())
				{
					tx.CreateTree("tree").Add("key1", new MemoryStream(buffer));
					tx.Commit();
				}
			}

			options = StorageEnvironmentOptions.ForPath(DataDir);
			options.MaxLogFileSize = 10 * options.PageSize;

			using (var env = new StorageEnvironment(options, NullLoggerSetup))
			{
				using (var tx = env.WriteTransaction())
				{
					tx.CreateTree("tree");
					tx.Commit();
				}

				using (var tx = env.ReadTransaction())
				{
					var read = tx.CreateTree("tree").Read("key1");
					Assert.NotNull(read);

					{
						Assert.Equal(buffer.Length, read.Reader.Length);
                        var bytes = read.Reader.ReadBytes(read.Reader.Length);
                        Assert.Equal(buffer, bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray());

                    }
                }
			}
		}
	}
}
