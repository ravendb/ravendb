using System;
using System.Runtime.InteropServices;

namespace Raven.Bundles.Encryption.Streams
{	/// <summary>
	/// Converts a struct to a byte array and back.
	/// </summary>
	public static class StructConverter
	{
		public static T ConvertBitsToStruct<T>(byte[] bytes, int? overrideHeaderSize = null) where T : struct
		{
			if (bytes == null)
				throw new ArgumentNullException("bytes");

			int size = overrideHeaderSize.HasValue ? overrideHeaderSize.Value : Marshal.SizeOf(typeof(T));
			if (size != bytes.Length)
				throw new ArgumentException("To convert a byte array to a " + typeof(T).FullName + ", the array must be of length " + size, "bytes");

			IntPtr ptr = Marshal.AllocHGlobal(size);
			T data;
			try
			{
				Marshal.Copy(bytes, 0, ptr, size);
				data = (T)Marshal.PtrToStructure(ptr, typeof(T));
			}
			finally
			{
				Marshal.FreeHGlobal(ptr);
			}

			return data;

		}

		public static byte[] ConvertStructToBits<T>(T data, int? overrideHeaderSize = null) where T : struct
		{
			int size = overrideHeaderSize.HasValue ? overrideHeaderSize.Value : Marshal.SizeOf(data);
			var arr = new byte[size];

			IntPtr ptr = Marshal.AllocHGlobal(size);
			try
			{
				Marshal.StructureToPtr(data, ptr, true);
				Marshal.Copy(ptr, arr, 0, size);
			}
			finally
			{
				Marshal.FreeHGlobal(ptr);
			}
			return arr;
		}
	}
}