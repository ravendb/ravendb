using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper
{
	public class NeedListGenerator : CriticalFinalizerObject, IDisposable
	{
		private const int ComparatorBufferSize = 0x8000000;
		private const int InputBufferSize = 0x100000;
		private readonly ReaderWriterLockSlim _disposerLock = new ReaderWriterLockSlim();
		private readonly IRdcLibrary _rdcLibrary;
		private readonly ISignatureRepository _seedSignatureRepository;
		private readonly ISignatureRepository _sourceSignatureRepository;
		private bool _disposed;

		public NeedListGenerator(ISignatureRepository seedSignatureRepository, ISignatureRepository sourceSignatureRepository)
		{
			try
			{
				_rdcLibrary = (IRdcLibrary)new RdcLibrary();
			}
			catch (InvalidCastException e)
			{
				throw new InvalidOperationException("This have to run in an MTA thread", e);
			}
			_seedSignatureRepository = seedSignatureRepository;
			_sourceSignatureRepository = sourceSignatureRepository;
		}

		public void Dispose()
		{
			_disposerLock.EnterWriteLock();
			try
			{
				if (_disposed)
					return;
				GC.SuppressFinalize(this);
				DisposeInternal();
			}
			finally
			{
				_disposed = true;
				_disposerLock.ExitWriteLock();
			}
		}

		public IList<RdcNeed> CreateNeedsList(SignatureInfo seedSignature, SignatureInfo sourceSignature)
		{
			return CreateNeedsList(seedSignature, sourceSignature, CancellationToken.None);
		}

		public IList<RdcNeed> CreateNeedsList(SignatureInfo seedSignature, SignatureInfo sourceSignature,
											  CancellationToken token)
		{
			var result = new List<RdcNeed>();
			using (var seedStream = _seedSignatureRepository.GetContentForReading(seedSignature.Name))
			using (var sourceStream = _sourceSignatureRepository.GetContentForReading(sourceSignature.Name))
			{
				var fileReader = (IRdcFileReader)new RdcFileReader(seedStream);
				IRdcComparator comparator;
				if (_rdcLibrary.CreateComparator(fileReader, ComparatorBufferSize, out comparator) != 0)
					throw new RdcException("Cannot create comparator");

				var inputBuffer = new RdcBufferPointer
				{
					Size = 0,
					Used = 0,
					Data = Marshal.AllocCoTaskMem(InputBufferSize + 16)
				};

				var outputBuffer = Marshal.AllocCoTaskMem(Marshal.SizeOf(typeof(RdcNeed)) * 256);

				try
				{
					var eofInput = false;
					var eofOutput = false;
					var outputPointer = new RdcNeedPointer();

					while (!eofOutput)
					{
						token.ThrowIfCancellationRequested();

						if (inputBuffer.Size == inputBuffer.Used && !eofInput)
						{
							var bytesRead = 0;
							try
							{
								bytesRead = RdcBufferTools.IntPtrCopy(sourceStream, inputBuffer.Data, InputBufferSize);
							}
							catch (Exception ex)
							{
								throw new RdcException("Failed to read from the source stream.", ex);
							}

							inputBuffer.Size = (uint)bytesRead;
							inputBuffer.Used = 0;

							if (bytesRead < InputBufferSize)
							{
								eofInput = true;
							}
						}

						// Initialize our output needs array
						outputPointer.Size = 256;
						outputPointer.Used = 0;
						outputPointer.Data = outputBuffer;

						RdcError error;

						var hr = comparator.Process(eofInput, ref eofOutput, ref inputBuffer, ref outputPointer, out error);

						if (hr != 0)
							throw new RdcException("Failed to process the signature block!", hr, error);

						// Convert the stream to a Needs array.
						var needs = GetRdcNeedList(outputPointer);
						result.AddRange(needs);
					}
				}
				finally
				{
					// Free our resources
					if (outputBuffer != IntPtr.Zero)
						Marshal.FreeCoTaskMem(outputBuffer);

					if (inputBuffer.Data != IntPtr.Zero)
						Marshal.FreeCoTaskMem(inputBuffer.Data);
				}
				return result;
			}
		}

		private static RdcNeed[] GetRdcNeedList(RdcNeedPointer pointer)
		{
			var result = new RdcNeed[pointer.Used];

			var ptr = pointer.Data;
			var needSize = Marshal.SizeOf(typeof(RdcNeed));

			// Get our native needs pointer 
			// and deserialize to our managed 
			// RdcNeed array.
			for (var i = 0; i < result.Length; i++)
			{
				result[i] = (RdcNeed)Marshal.PtrToStructure(ptr, typeof(RdcNeed));

				// Advance the intermediate pointer
				// to our next RdcNeed struct.
				ptr = new IntPtr(ptr.ToInt32() + needSize);
			}
			return result;
		}

		~NeedListGenerator()
		{
			try
			{
				Trace.WriteLine(
					"~NeedListGenerator: Disposing esent resources from finalizer! You should call Dispose() instead!");
				DisposeInternal();
			}
			catch (Exception exception)
			{
				try
				{
					Trace.WriteLine("Failed to dispose esent instance from finalizer because: " + exception);
				}
				catch
				{
				}
			}
		}

		private void DisposeInternal()
		{
			if (_rdcLibrary != null)
				Marshal.ReleaseComObject(_rdcLibrary);
		}
	}
}