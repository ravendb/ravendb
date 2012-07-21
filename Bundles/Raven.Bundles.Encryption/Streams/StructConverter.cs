using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Bundles.Encryption.Streams
{	/// <summary>
	/// Converst a struct to a byte array and back.
	/// </summary>
	public static class StructConverter
	{
		public static T ConvertBitsToStruct<T>(byte[] bytes) where T : struct
		{
			if (bytes == null)
				throw new ArgumentNullException("bytes");

			int size = Marshal.SizeOf(typeof(T));
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

		public static byte[] ConvertStructToBits<T>(T data) where T : struct
		{
			int size = Marshal.SizeOf(data);
			byte[] arr = new byte[size];

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
