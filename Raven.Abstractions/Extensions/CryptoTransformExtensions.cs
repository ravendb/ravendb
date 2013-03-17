#if !NETFX_CORE
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Extensions
{
	public static class CryptoTransformExtensions
	{
		public static byte[] TransformEntireBlock(this ICryptoTransform transform, byte[] data)
		{
			using (var input = new MemoryStream(data))
			using (var output = new CryptoStream(input, transform, CryptoStreamMode.Read))
			using (var result = new MemoryStream())
			{
				output.CopyTo(result);
				return result.ToArray();
			}
		}
	}
}
#endif