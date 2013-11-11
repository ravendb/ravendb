namespace Voron.Util
{
	using System;
	using System.Collections.Generic;
	using System.IO;

	internal class MultiBytePointer
	{
		private readonly LinkedList<BytePointer> _pointers;

		private LinkedListNode<BytePointer> _currentPointer;

		private int _availableLength;

		public int Count
		{
			get
			{
				return _pointers.Count;
			}
		}

		public unsafe byte* FirstPointer
		{
			get
			{
				if (_pointers.Count > 0)
					return _pointers.First.Value.Ptr;

				return null;
			}
		}

		public MultiBytePointer()
		{
			_pointers = new LinkedList<BytePointer>();
		}

		public unsafe void AddPointer(byte* ptr, int length)
		{
			_availableLength += length;

			var pointer = new BytePointer(ptr, length);

			_pointers.AddLast(pointer);

			if (_currentPointer == null)
				_currentPointer = _pointers.First;
		}

		public unsafe void Write(Stream value)
		{
			if (value.Length > _availableLength)
				throw new ArgumentException("Value is too long.");

			var buffer = new byte[4096];
			var lengthToWrite = value.Length;
			while (lengthToWrite > 0)
			{
				using (var stream = new UnmanagedMemoryStream(_currentPointer.Value.Ptr, _currentPointer.Value.AvailableLength, _currentPointer.Value.AvailableLength, FileAccess.ReadWrite))
				{
					do
					{
						var read = value.Read(buffer, 0, Math.Min(buffer.Length, _currentPointer.Value.AvailableLength));
						stream.Write(buffer, 0, read);

						lengthToWrite -= read;
						_currentPointer.Value.AvailableLength -= read;
					}
					while (_currentPointer.Value.AvailableLength > 0 && lengthToWrite > 0);
					
					_currentPointer = _currentPointer.Next;
				}
			}
		}

		private class BytePointer
		{
			public unsafe byte* Ptr { get; private set; }

			public int AvailableLength { get; set; }

			public unsafe BytePointer(byte* ptr, int length)
			{
				Ptr = ptr;
				AvailableLength = length;
			}
		}
	}
}