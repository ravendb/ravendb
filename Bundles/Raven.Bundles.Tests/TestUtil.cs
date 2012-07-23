using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client;
using Xunit;

namespace Raven.Bundles.Tests
{
	public static class TestUtil
	{
		public static void WaitForIndexing(IDocumentStore store)
		{
			while (store.DatabaseCommands.GetStatistics().StaleIndexes.Length > 0)
			{
				Thread.Sleep(100);
			}
		}

		public static bool ContainsSequence<T>(this IList<T> haystack, IList<T> needle)
		{
			int lastIndexToCheck = haystack.Count - needle.Count;
			for (int i = 0; i <= lastIndexToCheck; i++)
			{
				bool found = true;
				for (int j = 0; j < needle.Count; j++)
				{
					if (!haystack[i + j].Equals(needle[j]))
					{
						found = false;
						break;
					}
				}

				if (found)
					return true;
			}

			// not found at any index
			return false;
		}

		public static void AssertPlainTextIsNotSavedInAnyFileInPath(string[] plaintext, string path, Func<string, bool> filter)
		{
			// Asserts that the given string does not appear in any of the files in the database folder.
			byte[][] offendingBytes = plaintext.Select(Encoding.UTF8.GetBytes).ToArray();

			foreach (string file in Directory.GetFiles(path, "*", SearchOption.AllDirectories))
			{
				if(filter(file)==false)
					continue;
				byte[] contents = File.ReadAllBytes(file);

				foreach (var bytes in offendingBytes)
				{
					if (contents.ContainsSequence(bytes))
						Assert.False(true, string.Format("String \"{0}\" found in file {1}", Encoding.UTF8.GetString(bytes), file));
				}
			}
		}
	}
}
