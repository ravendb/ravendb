// -----------------------------------------------------------------------
//  <copyright file="IndexMessing.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;

namespace Raven.Tests.Indexes.Recovery
{
	public static class IndexMessing
	{
		/// <summary>
		/// This will cause CorruptIndexException("checksum mismatch in segments file")
		/// and Lucene will not be able to fix this index and the recovery action will be taken
		/// </summary>
		public static void MessSegmentsFile(string indexFullPath)
		{
			var segmentsFile = Directory.GetFiles(indexFullPath, "segments_*").First();

			using (var file = File.Open(segmentsFile, FileMode.Open))
			{
				file.Position = 10;
				file.Write(new byte[] { 1, 2, 3, 4, 5, 6 }, 0, 6);
			}
		}
	}
}