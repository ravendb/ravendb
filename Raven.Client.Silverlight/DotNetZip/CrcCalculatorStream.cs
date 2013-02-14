using System;

namespace Ionic.Zlib
{
	/// <summary>
	/// A Stream that calculates a CRC32 (a checksum) on all bytes read,
	/// or on all bytes written.
	/// </summary>
	///
	/// <remarks>
	/// <para>
	/// This class can be used to verify the CRC of a ZipEntry when
	/// reading from a stream, or to calculate a CRC when writing to a
	/// stream.  The stream should be used to either read, or write, but
	/// not both.  If you intermix reads and writes, the results are not
	/// defined.
	/// </para>
	///
	/// <para>
	/// This class is intended primarily for use internally by the
	/// DotNetZip library.
	/// </para>
	/// </remarks>
	public class CrcCalculatorStream : System.IO.Stream, System.IDisposable
	{
		private static readonly Int64 UnsetLengthLimit = -99;

		internal System.IO.Stream _innerStream;
		private CRC32 _Crc32;
		private Int64 _lengthLimit = -99;
		private bool _leaveOpen;

		/// <summary>
		/// Gets the total number of bytes run through the CRC32 calculator.
		/// </summary>
		///
		/// <remarks>
		/// This is either the total number of bytes read, or the total number of bytes
		/// written, depending on the direction of this stream.
		/// </remarks>
		public Int64 TotalBytesSlurped
		{
			get { return _Crc32.TotalBytesRead; }
		}


		/// <summary>
		/// The default constructor.
		/// </summary>
		/// <remarks>
		/// Instances returned from this constructor will leave the underlying stream
		/// open upon Close().
		/// </remarks>
		/// <param name="stream">The underlying stream</param>
		public CrcCalculatorStream(System.IO.Stream stream)
			: this(true, CrcCalculatorStream.UnsetLengthLimit, stream)
		{
		}


		/// <summary>
		/// The constructor allows the caller to specify how to handle the underlying
		/// stream at close.
		/// </summary>
		/// <param name="stream">The underlying stream</param>
		/// <param name="leaveOpen">true to leave the underlying stream
		/// open upon close of the CrcCalculatorStream.; false otherwise.</param>
		public CrcCalculatorStream(System.IO.Stream stream, bool leaveOpen)
			: this(leaveOpen, CrcCalculatorStream.UnsetLengthLimit, stream)
		{
		}


		/// <summary>
		/// A constructor allowing the specification of the length of the stream to read.
		/// </summary>
		/// <remarks>
		/// Instances returned from this constructor will leave the underlying stream open
		/// upon Close().
		/// </remarks>
		/// <param name="stream">The underlying stream</param>
		/// <param name="length">The length of the stream to slurp</param>
		public CrcCalculatorStream(System.IO.Stream stream, Int64 length)
			: this(true, length, stream)
		{
			if (length < 0)
				throw new ArgumentException("length");
		}

		/// <summary>
		/// A constructor allowing the specification of the length of the stream to
		/// read, as well as whether to keep the underlying stream open upon Close().
		/// </summary>
		/// <param name="stream">The underlying stream</param>
		/// <param name="length">The length of the stream to slurp</param>
		/// <param name="leaveOpen">true to leave the underlying stream
		/// open upon close of the CrcCalculatorStream.; false otherwise.</param>
		public CrcCalculatorStream(System.IO.Stream stream, Int64 length, bool leaveOpen)
			: this(leaveOpen, length, stream)
		{
			if (length < 0)
				throw new ArgumentException("length");
		}


		// This ctor is private - no validation is done here.  This is to allow the use
		// of a (specific) negative value for the _lengthLimit, to indicate that there
		// is no length set.  So we validate the length limit in those ctors that use an
		// explicit param, otherwise we don't validate, because it could be our special
		// value.
		private CrcCalculatorStream(bool leaveOpen, Int64 length, System.IO.Stream stream)
			: base()
		{
			_innerStream = stream;
			_Crc32 = new CRC32();
			_lengthLimit = length;
			_leaveOpen = leaveOpen;
		}

		/// <summary>
		/// Provides the current CRC for all blocks slurped in.
		/// </summary>
		public Int32 Crc
		{
			get { return _Crc32.Crc32Result; }
		}

		/// <summary>
		/// Indicates whether the underlying stream will be left open when the
		/// CrcCalculatorStream is Closed.
		/// </summary>
		public bool LeaveOpen
		{
			get { return _leaveOpen; }
			set { _leaveOpen = value; }
		}

		/// <summary>
		/// Read from the stream
		/// </summary>
		/// <param name="buffer">the buffer to read</param>
		/// <param name="offset">the offset at which to start</param>
		/// <param name="count">the number of bytes to read</param>
		/// <returns>the number of bytes actually read</returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			int bytesToRead = count;

			// Need to limit the # of bytes returned, if the stream is intended to have
			// a definite length.  This is especially useful when returning a stream for
			// the uncompressed data directly to the application.  The app won't
			// necessarily read only the UncompressedSize number of bytes.  For example
			// wrapping the stream returned from OpenReader() into a StreadReader() and
			// calling ReadToEnd() on it, We can "over-read" the zip data and get a
			// corrupt string.  The length limits that, prevents that problem.

			if (_lengthLimit != CrcCalculatorStream.UnsetLengthLimit)
			{
				if (_Crc32.TotalBytesRead >= _lengthLimit) return 0; // EOF
				Int64 bytesRemaining = _lengthLimit - _Crc32.TotalBytesRead;
				if (bytesRemaining < count) bytesToRead = (int)bytesRemaining;
			}
			int n = _innerStream.Read(buffer, offset, bytesToRead);
			if (n > 0) _Crc32.SlurpBlock(buffer, offset, n);
			return n;
		}

		/// <summary>
		/// Write to the stream.
		/// </summary>
		/// <param name="buffer">the buffer from which to write</param>
		/// <param name="offset">the offset at which to start writing</param>
		/// <param name="count">the number of bytes to write</param>
		public override void Write(byte[] buffer, int offset, int count)
		{
			if (count > 0) _Crc32.SlurpBlock(buffer, offset, count);
			_innerStream.Write(buffer, offset, count);
		}

		/// <summary>
		/// Indicates whether the stream supports reading.
		/// </summary>
		public override bool CanRead
		{
			get { return _innerStream.CanRead; }
		}

		/// <summary>
		/// Indicates whether the stream supports seeking.
		/// </summary>
		public override bool CanSeek
		{
			get { return _innerStream.CanSeek; }
		}

		/// <summary>
		/// Indicates whether the stream supports writing.
		/// </summary>
		public override bool CanWrite
		{
			get { return _innerStream.CanWrite; }
		}

		/// <summary>
		/// Flush the stream.
		/// </summary>
		public override void Flush()
		{
			_innerStream.Flush();
		}

		/// <summary>
		/// Not implemented.
		/// </summary>
		public override long Length
		{
			get
			{
				if (_lengthLimit == CrcCalculatorStream.UnsetLengthLimit)
					return _innerStream.Length;
				else return _lengthLimit;
			}
		}

		/// <summary>
		/// Not implemented.
		/// </summary>
		public override long Position
		{
			get { return _Crc32.TotalBytesRead; }
			set { throw new NotImplementedException(); }
		}

		/// <summary>
		/// Not implemented.
		/// </summary>
		/// <param name="offset">N/A</param>
		/// <param name="origin">N/A</param>
		/// <returns>N/A</returns>
		public override long Seek(long offset, System.IO.SeekOrigin origin)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Not implemented.
		/// </summary>
		/// <param name="value">N/A</param>
		public override void SetLength(long value)
		{
			throw new NotImplementedException();
		}


		void IDisposable.Dispose()
		{
			Close();
		}

		/// <summary>
		/// Closes the stream.
		/// </summary>
		public override void Close()
		{
			base.Close();
			if (!_leaveOpen)
				_innerStream.Close();
		}

	}
}