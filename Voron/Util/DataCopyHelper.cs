// -----------------------------------------------------------------------
//  <copyright file="DataCopyHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;

namespace Voron.Util
{
	public unsafe static class DataCopyHelper
	{
		internal static void ToStream(byte* ptr, long count, int bufferSize, Stream output)
		{
			var buffer = new byte[bufferSize];

			using (var stream = new UnmanagedMemoryStream(ptr, count))
			{
				while (stream.Position < stream.Length)
				{
					var read = stream.Read(buffer, 0, buffer.Length);
					output.Write(buffer, 0, read);
				}
			}
		}
	}
}