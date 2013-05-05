//#define Trace

// ParallelDeflateOutputStream.cs
// ------------------------------------------------------------------
//
// A DeflateStream that does compression only, it uses a
// divide-and-conquer approach with multiple threads to exploit multiple
// CPUs for the DEFLATE computation.
//
// last saved:
// Time-stamp: <2010-January-20 19:24:58>
// ------------------------------------------------------------------
//
// Copyright (c) 2009-2010 by Dino Chiesa
// All rights reserved!
//
// ------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Ionic.Zlib;
using System.IO;


namespace Ionic.Zlib
{
	/// <summary>
	///   A class for compressing and decompressing streams using the
	///   Deflate algorithm with multiple threads.
	/// </summary>
	///
	/// <remarks>
	/// <para>
	///   This class is for compression only, and that can be only
	///   through writing.
	/// </para>
	///
	/// <para>
	///   For more information on the Deflate algorithm, see IETF RFC 1951, "DEFLATE
	///   Compressed Data Format Specification version 1.3."
	/// </para>
	///
	/// <para>
	///   This class is similar to <see cref="Ionic.Zlib.DeflateStream"/>, except
	///   that this implementation uses an approach that employs multiple worker
	///   threads to perform the DEFLATE.  On a multi-cpu or multi-core computer,
	///   the performance of this class can be significantly higher than the
	///   single-threaded DeflateStream, particularly for larger streams.  How
	///   large?  Anything over 10mb is a good candidate for parallel compression.
	/// </para>
	///
	/// <para>
	///   The tradeoff is that this class uses more memory and more CPU than the
	///   vanilla DeflateStream, and also is less efficient as a compressor. For
	///   large files the size of the compressed data stream can be less than 1%
	///   larger than the size of a compressed data stream from the vanialla
	///   DeflateStream.  For smaller files the difference can be larger.  The
	///   difference will also be larger if you set the BufferSize to be lower
	///   than the default value.  Your mileage may vary. Finally, for small
	///   files, the ParallelDeflateOutputStream can be much slower than the vanilla
	///   DeflateStream, because of the overhead of using the thread pool.
	/// </para>
	///
	/// </remarks>
	/// <seealso cref="Ionic.Zlib.DeflateStream" />
	public class ParallelDeflateOutputStream : System.IO.Stream
	{

		private static readonly int IO_BUFFER_SIZE_DEFAULT = 64 * 1024;  // 128k

		private System.Collections.Generic.List<WorkItem> _pool;
		private bool                        _leaveOpen;
		private System.IO.Stream            _outStream;
		private int                         _nextToFill, _nextToWrite;
		private int                         _bufferSize = IO_BUFFER_SIZE_DEFAULT;
		private ManualResetEvent            _writingDone;
		private ManualResetEvent            _sessionReset;
		private bool                        _noMoreInputForThisSegment;
		private object                      _outputLock = new object();
		private bool                        _isClosed;
		private bool                        _isDisposed;
		private bool                        _firstWriteDone;
		private int                         _pc;
		private int                         _Crc32;
		private Int64                       _totalBytesProcessed;
		private Ionic.Zlib.CompressionLevel _compressLevel;
		private volatile Exception          _pendingException;
		private object                      _eLock = new Object();  // protects _pendingException

		// This bitfield is used only when Trace is defined.
		//private TraceBits _DesiredTrace = TraceBits.Write | TraceBits.WriteBegin |
		//TraceBits.WriteDone | TraceBits.Lifecycle | TraceBits.Fill | TraceBits.Flush |
		//TraceBits.Session;

		//private TraceBits _DesiredTrace = TraceBits.WriteBegin | TraceBits.WriteDone | TraceBits.Synch | TraceBits.Lifecycle  | TraceBits.Session ;

		private TraceBits _DesiredTrace = TraceBits.WriterThread | TraceBits.Synch | TraceBits.Lifecycle  | TraceBits.Session ;

		/// <summary>
		/// Create a ParallelDeflateOutputStream.
		/// </summary>
		/// <remarks>
		///
		/// <para>
		///   This stream compresses data written into it via the DEFLATE
		///   algorithm (see RFC 1951), and writes out the compressed byte stream.
		/// </para>
		///
		/// <para>
		///   The instance will use the default compression level, the default
		///   buffer sizes and the default number of threads and buffers per
		///   thread.
		/// </para>
		///
		/// <para>
		///   This class is similar to <see cref="Ionic.Zlib.DeflateStream"/>,
		///   except that this implementation uses an approach that employs
		///   multiple worker threads to perform the DEFLATE.  On a multi-cpu or
		///   multi-core computer, the performance of this class can be
		///   significantly higher than the single-threaded DeflateStream,
		///   particularly for larger streams.  How large?  Anything over 10mb is
		///   a good candidate for parallel compression.
		/// </para>
		///
		/// </remarks>
		///
		/// <example>
		///
		/// This example shows how to use a ParallelDeflateOutputStream to compress
		/// data.  It reads a file, compresses it, and writes the compressed data to
		/// a second, output file.
		///
		/// <code>
		/// byte[] buffer = new byte[WORKING_BUFFER_SIZE];
		/// int n= -1;
		/// String outputFile = fileToCompress + ".compressed";
		/// using (System.IO.Stream input = System.IO.File.OpenRead(fileToCompress))
		/// {
		///     using (var raw = System.IO.File.Create(outputFile))
		///     {
		///         using (Stream compressor = new ParallelDeflateOutputStream(raw))
		///         {
		///             while ((n= input.Read(buffer, 0, buffer.Length)) != 0)
		///             {
		///                 compressor.Write(buffer, 0, n);
		///             }
		///         }
		///     }
		/// }
		/// </code>
		/// <code lang="VB">
		/// Dim buffer As Byte() = New Byte(4096) {}
		/// Dim n As Integer = -1
		/// Dim outputFile As String = (fileToCompress &amp; ".compressed")
		/// Using input As Stream = File.OpenRead(fileToCompress)
		///     Using raw As FileStream = File.Create(outputFile)
		///         Using compressor As Stream = New ParallelDeflateOutputStream(raw)
		///             Do While (n &lt;&gt; 0)
		///                 If (n &gt; 0) Then
		///                     compressor.Write(buffer, 0, n)
		///                 End If
		///                 n = input.Read(buffer, 0, buffer.Length)
		///             Loop
		///         End Using
		///     End Using
		/// End Using
		/// </code>
		/// </example>
		/// <param name="stream">The stream to which compressed data will be written.</param>
		public ParallelDeflateOutputStream(System.IO.Stream stream)
			: this(stream, CompressionLevel.Default, CompressionStrategy.Default, false)
		{
		}

		/// <summary>
		///   Create a ParallelDeflateOutputStream using the specified CompressionLevel.
		/// </summary>
		/// <remarks>
		///   See the <see cref="ParallelDeflateOutputStream(System.IO.Stream)"/>
		///   constructor for example code.
		/// </remarks>
		/// <param name="stream">The stream to which compressed data will be written.</param>
		/// <param name="level">A tuning knob to trade speed for effectiveness.</param>
		public ParallelDeflateOutputStream(System.IO.Stream stream, CompressionLevel level)
			: this(stream, level, CompressionStrategy.Default, false)
		{
		}

		/// <summary>
		/// Create a ParallelDeflateOutputStream and specify whether to leave the captive stream open
		/// when the ParallelDeflateOutputStream is closed.
		/// </summary>
		/// <remarks>
		///   See the <see cref="ParallelDeflateOutputStream(System.IO.Stream)"/>
		///   constructor for example code.
		/// </remarks>
		/// <param name="stream">The stream to which compressed data will be written.</param>
		/// <param name="leaveOpen">
		///    true if the application would like the stream to remain open after inflation/deflation.
		/// </param>
		public ParallelDeflateOutputStream(System.IO.Stream stream, bool leaveOpen)
			: this(stream, CompressionLevel.Default, CompressionStrategy.Default, leaveOpen)
		{
		}

		/// <summary>
		/// Create a ParallelDeflateOutputStream and specify whether to leave the captive stream open
		/// when the ParallelDeflateOutputStream is closed.
		/// </summary>
		/// <remarks>
		///   See the <see cref="ParallelDeflateOutputStream(System.IO.Stream)"/>
		///   constructor for example code.
		/// </remarks>
		/// <param name="stream">The stream to which compressed data will be written.</param>
		/// <param name="level">A tuning knob to trade speed for effectiveness.</param>
		/// <param name="leaveOpen">
		///    true if the application would like the stream to remain open after inflation/deflation.
		/// </param>
		public ParallelDeflateOutputStream(System.IO.Stream stream, CompressionLevel level, bool leaveOpen)
			: this(stream, CompressionLevel.Default, CompressionStrategy.Default, leaveOpen)
		{
		}

		/// <summary>
		/// Create a ParallelDeflateOutputStream using the specified
		/// CompressionLevel and CompressionStrategy, and specifying whether to
		/// leave the captive stream open when the ParallelDeflateOutputStream is
		/// closed.
		/// </summary>
		/// <remarks>
		///   See the <see cref="ParallelDeflateOutputStream(System.IO.Stream)"/>
		///   constructor for example code.
		/// </remarks>
		/// <param name="stream">The stream to which compressed data will be written.</param>
		/// <param name="level">A tuning knob to trade speed for effectiveness.</param>
		/// <param name="strategy">
		///   By tweaking this parameter, you may be able to optimize the compression for
		///   data with particular characteristics.
		/// </param>
		/// <param name="leaveOpen">
		///    true if the application would like the stream to remain open after inflation/deflation.
		/// </param>
		public ParallelDeflateOutputStream(System.IO.Stream stream,
										   CompressionLevel level,
										   CompressionStrategy strategy,
										   bool leaveOpen)
		{
			TraceOutput(TraceBits.Lifecycle | TraceBits.Session, "-------------------------------------------------------");
			TraceOutput(TraceBits.Lifecycle | TraceBits.Session, "Create {0:X8}", this.GetHashCode());
			_compressLevel= level;
			_leaveOpen = leaveOpen;
			Strategy = strategy;

			BuffersPerCore = 4; // default

			_writingDone = new ManualResetEvent(false);
			_sessionReset = new ManualResetEvent(false);

			_outStream = stream;
		}


		/// <summary>
		///   The ZLIB strategy to be used during compression.
		/// </summary>
		///
		public CompressionStrategy Strategy
		{
			get;
			private set;
		}

		/// <summary>
		/// The number of buffers per CPU or CPU core.
		/// </summary>
		///
		/// <remarks>
		/// <para>
		///   This property sets the number of memory buffers to create, for every
		///   CPU or CPU core in the machine.  The divide-and-conquer approach
		///   taken by this class assumes a single thread from the application
		///   will call Write().  There will be multiple background threads that
		///   then compress (DEFLATE) the data written into the stream, and also a
		///   single output thread, also operating in the background, aggregating
		///   those results and finally emitting the output.
		/// </para>
		///
		/// <para>
		///   The default value is 4.  Different values may deliver better or
		///   worse results, depending on the dynamic performance characteristics
		///   of your storage and compute resources.
		/// </para>
		///
		/// <para>
		///   The total amount of storage space allocated for buffering will be
		///   (n*M*S*2), where n is the number of CPUs, M is the multiple (this
		///   property), S is the size of each buffer (<see cref="BufferSize"/>),
		///   and there are 2 buffers used by the compressor, one for input and
		///   one for output. For example, if your machine has 4 cores, and you
		///   set BuffersPerCore to 3, and you retain the default buffer size of
		///   128k, then the ParallelDeflateOutputStream will use 3mb of buffer
		///   memory in total.
		/// </para>
		///
		/// <para>
		///   The application can set this value at any time, but it is effective
		///   only before the first call to Write(), which is when the buffers are
		///   allocated.
		/// </para>
		/// </remarks>
		public int BuffersPerCore
		{
			get; set;
		}

		/// <summary>
		///   The size of the buffers used by the compressor threads.
		/// </summary>
		/// <remarks>
		///
		/// <para>
		///   The default buffer size is 128k. The application can set this value at any
		///   time, but it is effective only before the first Write().
		/// </para>
		///
		/// <para>
		///   Larger buffer sizes implies larger memory consumption but allows
		///   more efficient compression. Using smaller buffer sizes consumes less
		///   memory but result in less effective compression.  For example, using
		///   the default buffer size of 128k, the compression delivered is within
		///   1% of the compression delivered by the single-threaded <see
		///   cref="Ionic.Zlib.DeflateStream"/>.  On the other hand, using a
		///   BufferSize of 8k can result in a compressed data stream that is 5%
		///   larger than that delivered by the single-threaded
		///   <c>DeflateStream</c>.  Excessively small buffer sizes can also cause
		///   the speed of the ParallelDeflateOutputStream to drop, because of
		///   larger thread scheduling overhead dealing with many many small
		///   buffers.
		/// </para>
		///
		/// <para>
		///   The total amount of storage space allocated for buffering will be
		///   (n*M*S*2), where n is the number of CPUs, M is the multiple (<see
		///   cref="BuffersPerCore"/>), S is the size of each buffer (this
		///   property), and there are 2 buffers used by the compressor, one for
		///   input and one for output. For example, if your machine has a total
		///   of 4 cores, and if you set <see cref="BuffersPerCore"/> to 3, and
		///   you keep the default buffer size of 128k, then the
		///   <c>ParallelDeflateOutputStream</c> will use 3mb of buffer memory in
		///   total.
		/// </para>
		///
		/// </remarks>
		public int BufferSize
		{
			get { return _bufferSize;}
			set
			{
				if (value < 1024)
					throw new ArgumentException();
				_bufferSize = value;
			}
		}

		/// <summary>
		/// The CRC32 for the data that was written out, prior to compression.
		/// </summary>
		/// <remarks>
		/// This value is meaningful only after a call to Close().
		/// </remarks>
		public int Crc32 { get { return _Crc32; } }


		/// <summary>
		/// The total number of uncompressed bytes processed by the ParallelDeflateOutputStream.
		/// </summary>
		/// <remarks>
		/// This value is meaningful only after a call to Close().
		/// </remarks>
		public Int64 BytesProcessed { get { return _totalBytesProcessed; } }


		private void _InitializePoolOfWorkItems()
		{
			_pool = new System.Collections.Generic.List<WorkItem>();
			for(int i=0; i < BuffersPerCore * Environment.ProcessorCount; i++)
				_pool.Add(new WorkItem(_bufferSize, _compressLevel, Strategy));
			_pc = _pool.Count;

			for(int i=0; i < _pc; i++)
				_pool[i].index= i;

			// set the pointers
			_nextToFill= _nextToWrite= 0;
		}


		private void _KickoffWriter()
		{
		    Task.Factory.StartNew(this._PerpetualWriterMethod);
		}


		/// <summary>
		///   Write data to the stream.
		/// </summary>
		///
		/// <remarks>
		///
		/// <para>
		///   To use the ParallelDeflateOutputStream to compress data, create a
		///   ParallelDeflateOutputStream with CompressionMode.Compress, passing a
		///   writable output stream.  Then call Write() on that
		///   ParallelDeflateOutputStream, providing uncompressed data as input.  The
		///   data sent to the output stream will be the compressed form of the data
		///   written.
		/// </para>
		///
		/// <para>
		///   To decompress data, use the <see cref="Ionic.Zlib.DeflateStream"/> class.
		/// </para>
		///
		/// </remarks>
		/// <param name="buffer">The buffer holding data to write to the stream.</param>
		/// <param name="offset">the offset within that data array to find the first byte to write.</param>
		/// <param name="count">the number of bytes to write.</param>
		public override void Write(byte[] buffer, int offset, int count)
		{
			// Fill a work buffer; when full, flip state to 'Filled'

			if (_isClosed)
				throw new NotSupportedException();

			// dispense any exceptions that occurred on the BG threads
			if (_pendingException != null)
				throw _pendingException;

			if (count == 0) return;

			if (!_firstWriteDone)
			{
				// Want to do this on first Write, first session, and not in the
				// constructor.  We want to allow the BufferSize and BuffersPerCore to
				// change after construction, but before first Write.
				_InitializePoolOfWorkItems();

				// Only do this once (ever), the first time Write() is called:
				_KickoffWriter();

				// Release the writer thread.
				TraceOutput(TraceBits.Synch, "Synch    _sessionReset.Set()          Write (first)");
				_sessionReset.Set();

				_firstWriteDone = true;
			}


			do
			{
				int ix = _nextToFill % _pc;
				WorkItem workitem = _pool[ix];
				lock(workitem)
				{
					TraceOutput(TraceBits.Fill,
								   "Fill     lock     wi({0}) stat({1}) iba({2}) nf({3})",
								   workitem.index,
								   workitem.status,
								   workitem.inputBytesAvailable,
								   _nextToFill
								   );

					// If the status is what we want, then use the workitem.
					if (workitem.status == (int)WorkItem.Status.None ||
						workitem.status == (int)WorkItem.Status.Done ||
						workitem.status == (int)WorkItem.Status.Filling)
					{
						workitem.status = (int)WorkItem.Status.Filling;
						int limit = ((workitem.buffer.Length - workitem.inputBytesAvailable) > count)
							? count
							: (workitem.buffer.Length - workitem.inputBytesAvailable);

						// copy from the provided buffer to our workitem, starting at
						// the tail end of whatever data we might have in there currently.
						Array.Copy(buffer, offset, workitem.buffer, workitem.inputBytesAvailable, limit);

						count -= limit;
						offset += limit;
						workitem.inputBytesAvailable += limit;
						if (workitem.inputBytesAvailable==workitem.buffer.Length)
						{
							workitem.status = (int)WorkItem.Status.Filled;
							// No need for interlocked.increment: the Write() method
							// is documented as not multi-thread safe, so we can assume Write()
							// calls come in from only one thread.
							_nextToFill++;

							TraceOutput(TraceBits.Fill,
										   "Fill     QUWI     wi({0}) stat({1}) iba({2}) nf({3})",
										   workitem.index,
										   workitem.status,
										   workitem.inputBytesAvailable,
										   _nextToFill
										   );

                            Task.Factory.StartNew(() => _DeflateOne(workitem));
								
						}

					}
					else
					{
						int wcycles= 0;

						while (workitem.status != (int)WorkItem.Status.None &&
							   workitem.status != (int)WorkItem.Status.Done &&
							   workitem.status != (int)WorkItem.Status.Filling)
						{
							TraceOutput(TraceBits.Fill,
										   "Fill     waiting  wi({0}) stat({1}) nf({2})",
										   workitem.index,
										   workitem.status,
										   _nextToFill);

							wcycles++;

							Monitor.Pulse(workitem);
							Monitor.Wait(workitem);

							if (workitem.status == (int)WorkItem.Status.None ||
								workitem.status == (int)WorkItem.Status.Done ||
								workitem.status == (int)WorkItem.Status.Filling)
								TraceOutput(TraceBits.Fill,
											   "Fill     A-OK     wi({0}) stat({1}) iba({2}) cyc({3})",
											   workitem.index,
											   workitem.status,
											   workitem.inputBytesAvailable,
											   wcycles);
						}
					}
				}
			}
			while (count > 0);  // until no more to write

			return;
		}



		/// <summary>
		/// Flush the stream.
		/// </summary>
		public override void Flush()
		{
			_Flush(false);
		}


		private void _Flush(bool lastInput)
		{
			if (_isClosed)
				throw new NotSupportedException();

			// pass any partial buffer out to the compressor workers:
			WorkItem workitem = _pool[_nextToFill % _pc];
			lock(workitem)
			{
				if ( workitem.status == (int)WorkItem.Status.Filling)
				{
					workitem.status = (int)WorkItem.Status.Filled;
					_nextToFill++;

					// When flush is called from Close(), we set _noMore.
					// can't do it before updating nextToFill, though.
					if (lastInput)
						_noMoreInputForThisSegment= true;

					TraceOutput(TraceBits.Flush,
								   "Flush    filled   wi({0})  iba({1}) nf({2}) nomore({3})",
								   workitem.index, workitem.inputBytesAvailable, _nextToFill, _noMoreInputForThisSegment);

                    Task.Factory.StartNew(() => _DeflateOne(workitem));
						

					//Monitor.Pulse(workitem);
				}
				else
				{
					// When flush is called from Close(), we set _noMore.
					// Gotta do this whether or not there is another packet to send along.
					if (lastInput)
						_noMoreInputForThisSegment= true;

					TraceOutput(TraceBits.Flush,
								   "Flush    noaction wi({0}) stat({1}) nf({2})  nomore({3})",
								   workitem.index, workitem.status, _nextToFill, _noMoreInputForThisSegment);
				}
			}
		}


		/// <summary>
		/// Close the stream.
		/// </summary>
		/// <remarks>
		/// You must call Close on the stream to guarantee that all of the data written in has
		/// been compressed, and the compressed data has been written out.
		/// </remarks>
		public override void Close()
		{
			TraceOutput(TraceBits.Session, "Close {0:X8}", this.GetHashCode());

			if (_isClosed) return;

			_Flush(true);

			//System.Diagnostics.StackTrace st = new System.Diagnostics.StackTrace(1);
			//System.Console.WriteLine(st.ToString());

			// need to get Writer off the workitem, in case he's waiting forever
			WorkItem workitem = _pool[_nextToFill % _pc];
			lock(workitem)
			{
				Monitor.PulseAll(workitem);
			}

			// wait for the writer to complete his work
			TraceOutput(TraceBits.Synch, "Synch    _writingDone.WaitOne(begin)  Close");
			_writingDone.WaitOne();
			TraceOutput(TraceBits.Synch, "Synch    _writingDone.WaitOne(done)   Close");

			TraceOutput(TraceBits.Session, "-------------------------------------------------------");
			if (!_leaveOpen)
				_outStream.Close();

			_isClosed= true;
		}



//        /// <summary>The destructor</summary>
//         ~ParallelDeflateOutputStream()
//         {
//             TraceOutput(TraceBits.Lifecycle, "Destructor  {0:X8}", this.GetHashCode());
//             // call Dispose with false.  Since we're in the
//             // destructor call, the managed resources will be
//             // disposed of anyways.
//             Dispose(false);
//         }



		// workitem 10030 - implement a new Dispose method

		/// <summary>Dispose the object</summary>
		/// <remarks>
		///   <para>
		///     Because ParallelDeflateOutputStream is IDisposable, the
		///     application must call this method when finished using the instance.
		///   </para>
		///   <para>
		///     This method is generally called implicitly upon exit from
		///     a <c>using</c> scope in C# (<c>Using</c> in VB).
		///   </para>
		/// </remarks>
		new public void  Dispose()
		{
			TraceOutput(TraceBits.Lifecycle, "Dispose  {0:X8}", this.GetHashCode());
			_isDisposed= true;
			_pool = null;
			TraceOutput(TraceBits.Synch, "Synch    _sessionReset.Set()  Dispose");
			_sessionReset.Set();  // tell writer to die
			Dispose(true);
		}



		/// <summary>The Dispose method</summary>
		protected override void Dispose(bool disposeManagedResources)
		{
			if (disposeManagedResources)
			{
				// dispose managed resources
				_writingDone.Close();
				_sessionReset.Close();
			}
		}


		/// <summary>
		///   Resets the stream for use with another stream.
		/// </summary>
		/// <remarks>
		///   Because the ParallelDeflateOutputStream is expensive to create, it
		///   has been designed so that it can be recycled and re-used.  You have
		///   to call Close() on the stream first, then you can call Reset() on
		///   it, to use it again on another stream.
		/// </remarks>
		///
		/// <example>
		/// <code>
		/// ParallelDeflateOutputStream deflater = null;
		/// foreach (var inputFile in listOfFiles)
		/// {
		///     string outputFile = inputFile + ".compressed";
		///     using (System.IO.Stream input = System.IO.File.OpenRead(inputFile))
		///     {
		///         using (var outStream = System.IO.File.Create(outputFile))
		///         {
		///             if (deflater == null)
		///                 deflater = new ParallelDeflateOutputStream(outStream,
		///                                                            CompressionLevel.Best,
		///                                                            CompressionStrategy.Default,
		///                                                            true);
		///             deflater.Reset(outStream);
		///
		///             while ((n= input.Read(buffer, 0, buffer.Length)) != 0)
		///             {
		///                 deflater.Write(buffer, 0, n);
		///             }
		///         }
		///     }
		/// }
		/// </code>
		/// </example>
		public void Reset(Stream stream)
		{
			TraceOutput(TraceBits.Session, "-------------------------------------------------------");
			TraceOutput(TraceBits.Session, "Reset {0:X8} firstDone({1})", this.GetHashCode(), _firstWriteDone);

			if (!_firstWriteDone) return;

			if (_noMoreInputForThisSegment)
			{
				// wait til done writing:
				TraceOutput(TraceBits.Synch, "Synch    _writingDone.WaitOne(begin)  Reset");
				_writingDone.WaitOne();
				TraceOutput(TraceBits.Synch, "Synch    _writingDone.WaitOne(done)   Reset");

				// reset all status
				foreach (var workitem in _pool)
					workitem.status = (int) WorkItem.Status.None;

				_noMoreInputForThisSegment= false;
				_nextToFill= _nextToWrite= 0;
				_totalBytesProcessed = 0L;
				_Crc32= 0;
				_isClosed= false;

				TraceOutput(TraceBits.Synch, "Synch    _writingDone.Reset()         Reset");
				_writingDone.Reset();
			}
			else
			{
				TraceOutput(TraceBits.Synch, "Synch                           Reset noMore=false");
			}

			_outStream = stream;

			// release the writer thread for the next "session"
			TraceOutput(TraceBits.Synch, "Synch    _sessionReset.Set()          Reset");
			_sessionReset.Set();
		}



		private void _PerpetualWriterMethod()
		{
			TraceOutput(TraceBits.WriterThread, "_PerpetualWriterMethod START");

			try
			{
				do
				{
					// wait for the next session
					TraceOutput(TraceBits.Synch | TraceBits.WriterThread, "Synch    _sessionReset.WaitOne(begin) PWM");
					_sessionReset.WaitOne();
					TraceOutput(TraceBits.Synch | TraceBits.WriterThread, "Synch    _sessionReset.WaitOne(done)  PWM");

					if (_isDisposed) break;

					TraceOutput(TraceBits.Synch | TraceBits.WriterThread, "Synch    _sessionReset.Reset()        PWM");
					_sessionReset.Reset();

					// repeatedly write buffers as they become ready
					WorkItem workitem = null;
					Ionic.Zlib.CRC32 c= new Ionic.Zlib.CRC32();
					do
					{
						workitem = _pool[_nextToWrite % _pc];
						lock(workitem)
						{
							if (_noMoreInputForThisSegment)
								TraceOutput(TraceBits.Write,
											   "Write    drain    wi({0}) stat({1}) canuse({2})  cba({3})",
											   workitem.index,
											   workitem.status,
											   (workitem.status == (int)WorkItem.Status.Compressed),
											   workitem.compressedBytesAvailable);

							do
							{
								if (workitem.status == (int)WorkItem.Status.Compressed)
								{
									TraceOutput(TraceBits.WriteBegin,
												   "Write    begin    wi({0}) stat({1})              cba({2})",
												   workitem.index,
												   workitem.status,
												   workitem.compressedBytesAvailable);

									workitem.status = (int)WorkItem.Status.Writing;
									_outStream.Write(workitem.compressed, 0, workitem.compressedBytesAvailable);
									c.Combine(workitem.crc, workitem.inputBytesAvailable);
									_totalBytesProcessed += workitem.inputBytesAvailable;
									_nextToWrite++;
									workitem.inputBytesAvailable= 0;
									workitem.status = (int)WorkItem.Status.Done;

									TraceOutput(TraceBits.WriteDone,
												   "Write    done     wi({0}) stat({1})              cba({2})",
												   workitem.index,
												   workitem.status,
												   workitem.compressedBytesAvailable);


									Monitor.Pulse(workitem);
									break;
								}
								else
								{
									int wcycles = 0;
									// I've locked a workitem I cannot use.
									// Therefore, wake someone else up, and then release the lock.
									while (workitem.status != (int)WorkItem.Status.Compressed)
									{
										TraceOutput(TraceBits.WriteWait,
													   "Write    waiting  wi({0}) stat({1}) nw({2}) nf({3}) nomore({4})",
													   workitem.index,
													   workitem.status,
													   _nextToWrite, _nextToFill,
													   _noMoreInputForThisSegment );

										if (_noMoreInputForThisSegment && _nextToWrite == _nextToFill)
											break;

										wcycles++;

										// wake up someone else
										Monitor.Pulse(workitem);
										// release and wait
										Monitor.Wait(workitem);

										if (workitem.status == (int)WorkItem.Status.Compressed)
											TraceOutput(TraceBits.WriteWait,
														   "Write    A-OK     wi({0}) stat({1}) iba({2}) cba({3}) cyc({4})",
														   workitem.index,
														   workitem.status,
														   workitem.inputBytesAvailable,
														   workitem.compressedBytesAvailable,
														   wcycles);
									}

									if (_noMoreInputForThisSegment && _nextToWrite == _nextToFill)
										break;

								}
							}
							while (true);
						}

						if (_noMoreInputForThisSegment)
							TraceOutput(TraceBits.Write,
										   "Write    nomore  nw({0}) nf({1}) break({2})",
										   _nextToWrite, _nextToFill, (_nextToWrite == _nextToFill));

						if (_noMoreInputForThisSegment && _nextToWrite == _nextToFill)
							break;

					} while (true);


					// Finish:
					// After writing a series of buffers, closing each one with
					// Flush.Sync, we now write the final one as Flush.Finish, and
					// then stop.
					byte[] buffer = new byte[128];
					ZlibCodec compressor = new ZlibCodec();
					int rc = compressor.InitializeDeflate(_compressLevel, false);
					compressor.InputBuffer = null;
					compressor.NextIn = 0;
					compressor.AvailableBytesIn = 0;
					compressor.OutputBuffer = buffer;
					compressor.NextOut = 0;
					compressor.AvailableBytesOut = buffer.Length;
					rc = compressor.Deflate(FlushType.Finish);

					if (rc != ZlibConstants.Z_STREAM_END && rc != ZlibConstants.Z_OK)
						throw new Exception("deflating: " + compressor.Message);

					if (buffer.Length - compressor.AvailableBytesOut > 0)
					{
						TraceOutput(TraceBits.WriteBegin,
									   "Write    begin    flush bytes({0})",
									   buffer.Length - compressor.AvailableBytesOut);

						_outStream.Write(buffer, 0, buffer.Length - compressor.AvailableBytesOut);

						TraceOutput(TraceBits.WriteBegin,
									   "Write    done     flush");
					}

					compressor.EndDeflate();

					_Crc32 = c.Crc32Result;

					// signal that writing is complete:
					TraceOutput(TraceBits.Synch, "Synch    _writingDone.Set()           PWM");
					_writingDone.Set();
				}
				while (true);
			}
			catch (System.Exception exc1)
			{
				lock(_eLock)
				{
					// expose the exception to the main thread
					if (_pendingException!=null)
						_pendingException = exc1;
				}
			}

			TraceOutput(TraceBits.WriterThread, "_PerpetualWriterMethod FINIS");
		}




		private void _DeflateOne(Object wi)
		{
			WorkItem workitem = (WorkItem) wi;
			try
			{
				// compress one buffer
				int myItem = workitem.index;

				lock(workitem)
				{
					if (workitem.status != (int)WorkItem.Status.Filled)
						throw new InvalidOperationException();

					Ionic.Zlib.CRC32 crc = new CRC32();

					// use the workitem:
					// calc CRC on the buffer
					crc.SlurpBlock(workitem.buffer, 0, workitem.inputBytesAvailable);

					// deflate it
					DeflateOneSegment(workitem);

					// update status
					workitem.status = (int)WorkItem.Status.Compressed;
					workitem.crc = crc.Crc32Result;

					TraceOutput(TraceBits.Compress,
								   "Compress          wi({0}) stat({1}) len({2})",
								   workitem.index,
								   workitem.status,
								   workitem.compressedBytesAvailable
								   );

					// release the item
					Monitor.Pulse(workitem);
				}
			}
			catch (System.Exception exc1)
			{
				lock(_eLock)
				{
					// expose the exception to the main thread
					if (_pendingException!=null)
						_pendingException = exc1;
				}
			}
		}




		private bool DeflateOneSegment(WorkItem workitem)
		{
			ZlibCodec compressor = workitem.compressor;
			int rc= 0;
			compressor.ResetDeflate();
			compressor.NextIn = 0;

			compressor.AvailableBytesIn = workitem.inputBytesAvailable;

			// step 1: deflate the buffer
			compressor.NextOut = 0;
			compressor.AvailableBytesOut =  workitem.compressed.Length;
			do
			{
				compressor.Deflate(FlushType.None);
			}
			while (compressor.AvailableBytesIn > 0 || compressor.AvailableBytesOut == 0);

			// step 2: flush (sync)
			rc = compressor.Deflate(FlushType.Sync);

			workitem.compressedBytesAvailable= (int) compressor.TotalBytesOut;
			return true;
		}


		[System.Diagnostics.ConditionalAttribute("Trace")]
		private void TraceOutput(TraceBits bits, string format, params object[] varParams)
		{
			if ((bits & _DesiredTrace) != 0)
			{
				lock(_outputLock)
				{
					int tid = Thread.CurrentThread.GetHashCode();
#if !SILVERLIGHT
					Console.ForegroundColor = (ConsoleColor) (tid % 8 + 8);
#endif
					Console.Write("{0:000} PDOS ", tid);
					Console.WriteLine(format, varParams);
#if !SILVERLIGHT
					Console.ResetColor();
#endif
				}
			}
		}


		// used only when Trace is defined
		[Flags]
		enum TraceBits
		{
			None         = 0,
			Write        = 1,    // write out
			WriteBegin   = 2,    // begin to write out
			WriteDone    = 4,    // done writing out
			WriteWait    = 8,    // write thread waiting for buffer
			Flush        = 16,
			Compress     = 32,   // async compress
			Fill         = 64,   // filling buffers, when caller invokes Write()
			Lifecycle    = 128,  // constructor/disposer
			Session      = 256,  // Close/Reset
			Synch        = 512,  // thread synchronization
			WriterThread = 1024, // writer thread
		}



		/// <summary>
		/// Indicates whether the stream supports Seek operations.
		/// </summary>
		/// <remarks>
		/// Always returns false.
		/// </remarks>
		public override bool CanSeek
		{
			get { return false; }
		}


		/// <summary>
		/// Indicates whether the stream supports Read operations.
		/// </summary>
		/// <remarks>
		/// Always returns false.
		/// </remarks>
		public override bool CanRead
		{
			get {return false;}
		}

		/// <summary>
		/// Indicates whether the stream supports Write operations.
		/// </summary>
		/// <remarks>
		/// Returns true if the provided stream is writable.
		/// </remarks>
		public override bool CanWrite
		{
			get { return _outStream.CanWrite; }
		}

		/// <summary>
		/// Reading this property always throws a NotImplementedException.
		/// </summary>
		public override long Length
		{
			get { throw new NotImplementedException(); }
		}

		/// <summary>
		/// Reading or Writing this property always throws a NotImplementedException.
		/// </summary>
		public override long Position
		{
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		/// <summary>
		/// This method always throws a NotImplementedException.
		/// </summary>
		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// This method always throws a NotImplementedException.
		/// </summary>
		public override long Seek(long offset, System.IO.SeekOrigin origin)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// This method always throws a NotImplementedException.
		/// </summary>
		public override void SetLength(long value)
		{
			throw new NotImplementedException();
		}

	}

}


