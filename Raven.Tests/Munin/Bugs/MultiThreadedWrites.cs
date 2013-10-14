using System;
using System.Linq;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests.Bugs
{
	using Raven.Abstractions.Util.Encryptors;
	using Raven.Tests.Helpers;

	public class MultiThreadedWrites : IDisposable
	{
		public MultiThreadedWrites()
		{
			Encryptor.Initialize(SettingsHelper.UseFipsEncryptionAlgorithms);
		}

		[Fact]
		public void MultipleThreadsCanSafelyWriteandCommit()
		{
			var tempPath = Path.GetTempPath();
			var dbPath = Path.Combine(tempPath, "test" + ".ravendb");
			Log("Saving db in: " + dbPath);
			File.Delete(dbPath);
			var persistentSource = new FileBasedPersistentSource(tempPath, "test", writeThrough: true);
			var database = new Database(persistentSource);
			var tableOne = database.Add(new Table("Test1"));
			var tableTwo = database.Add(new Table("Test2"));
			database.Initialize();

			try
			{
				Parallel.For(0, 10, counter =>
				{
					Table table = counter % 2 == 0 ? tableOne : tableTwo;
					ProcessTask(counter, database, table);
				});
			}
			catch (AggregateException aggEx)
			{
				Assert.False(true, aggEx.Message + " : " + aggEx.InnerException.Message);
			}
			finally
			{
				persistentSource.Dispose();
			}
		}

		private static void ProcessTask(int counter, Database database, Table table)
		{
			var localData = new byte[] { 1, 2, 4, 5 }.Select(x => (byte)(x * counter)).ToArray();
			var localKey = "key" + counter.ToString();
			var thrId = Thread.CurrentThread.ManagedThreadId;
			Log("Thread {0,2}, counter = {1}, localKey = {2}, localData = {3}",
				thrId, counter, localKey, String.Join(", ", localData));

			using (database.BeginTransaction())
			{
				Assert.True(table.Put(RavenJToken.FromObject(localKey), localData));

				for (int i = 0; i < 50; i++)
				{
					Assert.True(table.Put(RavenJToken.FromObject(localKey + "_" + i), localData));
				}

				database.Commit();
			}

			database.Compact();
		}

		private static void Log(string format, params object[] args)
		{
			Trace.WriteLine(String.Format(format, args));
		}

		public void Dispose()
		{
			Encryptor.Dispose();
		}
	}
}