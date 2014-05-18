using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Threading;
using NLog;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper.Unmanaged;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper
{
	public class SigGenerator : CriticalFinalizerObject, IDisposable
	{
		private const uint OutputBufferSize = 1024;
		private const int InputBufferSize = 8 * 1024;
		private static readonly Logger log = LogManager.GetCurrentClassLogger();

		private readonly ReaderWriterLockSlim _disposerLock = new ReaderWriterLockSlim();

		private readonly IRdcLibrary _rdcLibrary;
		private bool _disposed;
		private int _recursionDepth;

		public SigGenerator()
		{
			try
			{
				_rdcLibrary = (IRdcLibrary)new RdcLibrary();
			}
			catch (InvalidCastException e)
			{
				throw new InvalidOperationException("This code must run in an MTA thread", e);
			}
			catch (COMException comException)
			{
				log.ErrorException("Remote Differential Compression feature is not installed", comException);
				throw new NotSupportedException("Remote Differential Compression feature is not installed", comException);
			}
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

		public IList<SignatureInfo> GenerateSignatures(Stream source, string fileName,
													   ISignatureRepository signatureRepository)
		{
			_recursionDepth = EvaluateRecursionDepth(source);
			if (_recursionDepth == 0)
				return new List<SignatureInfo>();

			var rdcGenerator = InitializeRdcGenerator();
			try
			{
				return Process(source, rdcGenerator, fileName, signatureRepository);
			}
			finally
			{
				Marshal.ReleaseComObject(rdcGenerator);
			}
		}

		private IList<SignatureInfo> Process(Stream source, IRdcGenerator rdcGenerator, string fileName,
											 ISignatureRepository signatureRepository)
		{
			var result = Enumerable.Range(0, _recursionDepth).Reverse().Select(i => new SignatureInfo(i, fileName)).ToList();

			var eof = false;
			var eofOutput = false;
			// prepare streams
			var sigStreams = result.Select(item => signatureRepository.CreateContent(item.Name)).ToList();

			var inputBuffer = new RdcBufferPointer
			{
				Size = 0,
				Used = 0,
				Data = Marshal.AllocCoTaskMem(InputBufferSize + 16)
			};

			var rdcBufferPointers = PrepareRdcBufferPointers();
			var outputPointers = PrepareOutputPointers(rdcBufferPointers);
			try
			{
				while (!eofOutput)
				{
					if (inputBuffer.Size == inputBuffer.Used)
						inputBuffer = GetInputBuffer(source, InputBufferSize, inputBuffer, ref eof);

					RdcError rdcErrorCode;
					var hr = rdcGenerator.Process(
						eof,
						ref eofOutput,
						ref inputBuffer,
						(uint)_recursionDepth,
						outputPointers,
						out rdcErrorCode);

					if (hr != 0)
						throw new RdcException("RdcGenerator failed to process the signature block.", hr, rdcErrorCode);

					RdcBufferTranslate(outputPointers, rdcBufferPointers);

					for (var i = 0; i < _recursionDepth; i++)
					{
						var resultStream = sigStreams[i];

						// Write the signature block to the file.
						var bytesWritten = RdcBufferTools.IntPtrCopy(
							rdcBufferPointers[i].Data,
							resultStream,
							(int)rdcBufferPointers[i].Used);
						result[i].Length += bytesWritten;

						if (rdcBufferPointers[i].Used != bytesWritten)
						{
							throw new RdcException("Failed to write to the signature file.");
						}


						rdcBufferPointers[i].Used = 0;
					}

					RdcBufferTranslate(rdcBufferPointers, outputPointers);
				}
			}
			finally
			{
				if (inputBuffer.Data != IntPtr.Zero)
					Marshal.FreeCoTaskMem(inputBuffer.Data);

				foreach (var item in outputPointers)
				{
					Marshal.FreeCoTaskMem(item);
				}
			}
			result.Reverse();
			signatureRepository.Flush(result);
			return result;
		}

		private static RdcBufferPointer GetInputBuffer(Stream source, int inputBufferSize, RdcBufferPointer inputBuffer,
													   ref bool eof)
		{
			if (eof)
			{
				inputBuffer.Size = 0;
				inputBuffer.Used = 0;
			}
			else
			{
				var bytesRead = 0;
				try
				{
					bytesRead = RdcBufferTools.IntPtrCopy(source, inputBuffer.Data, inputBufferSize);
				}
				catch (Exception ex)
				{
					throw new RdcException("Failed to read from the source stream.", ex);
				}

				inputBuffer.Size = (uint)bytesRead;
				inputBuffer.Used = 0;

				if (bytesRead < inputBufferSize)
				{
					eof = true;
				}
			}
			return inputBuffer;
		}

		private static void RdcBufferTranslate(RdcBufferPointer[] source, IntPtr[] dest)
		{
			if (source.Length != dest.Length)
			{
				throw new ArgumentException("source and dest should have the same length");
			}
			// Marshal the managed structure to a native
			// pointer and add it to our array.
			for (var i = 0; i < dest.Length; i++)
			{
				dest[i] = Marshal.AllocCoTaskMem(Marshal.SizeOf(source[i]));
				Marshal.StructureToPtr(source[i], dest[i], false);
			}
		}

		private static void RdcBufferTranslate(IntPtr[] source, RdcBufferPointer[] dest)
		{
			if (source.Length != dest.Length)
			{
				throw new ArgumentException("source and dest should have the same length");
			}
			// Marshal the native pointer back to the 
			// managed structure.
			for (var i = 0; i < dest.Length; i++)
			{
				dest[i] = (RdcBufferPointer)Marshal.PtrToStructure(source[i], typeof(RdcBufferPointer));
				Marshal.FreeCoTaskMem(source[i]);
			}
		}

		private static IntPtr[] PrepareOutputPointers(RdcBufferPointer[] rdcBufferPointers)
		{
			var result = new IntPtr[rdcBufferPointers.Length];
			for (var i = 0; i < rdcBufferPointers.Length; i++)
			{
				result[i] = Marshal.AllocCoTaskMem(Marshal.SizeOf(rdcBufferPointers[i]));
				Marshal.StructureToPtr(rdcBufferPointers[i], result[i], false);
			}
			return result;
		}

		private RdcBufferPointer[] PrepareRdcBufferPointers()
		{
			var outputBuffers = PrepareOutputBuffers();

			var result = new RdcBufferPointer[outputBuffers.Length];
			for (var i = 0; i < outputBuffers.Length; i++)
			{
				result[i].Size = OutputBufferSize;
				result[i].Data = outputBuffers[i];
				result[i].Used = 0;
			}
			return result;
		}

		private IntPtr[] PrepareOutputBuffers()
		{
			var outputBuffers = new IntPtr[_recursionDepth];
			for (var i = 0; i < _recursionDepth; i++)
			{
				outputBuffers[i] = Marshal.AllocCoTaskMem((int)OutputBufferSize + 16);
			}
			return outputBuffers;
		}

		private IRdcGenerator InitializeRdcGenerator()
		{
			var generatorParameterses = InitializeGeneratorParameterses();
			IRdcGenerator result;
			var hr = _rdcLibrary.CreateGenerator((uint)_recursionDepth, generatorParameterses, out result);
			if (hr != 0)
			{
				throw new RdcException("Failed to create the RdcGenerator.", hr);
			}

			// Enable similarity
			((IRdcSimilarityGenerator)result).EnableSimilarity();
			return result;
		}

		public int EvaluateRecursionDepth(Stream source)
		{
			int result;
			var hr = _rdcLibrary.ComputeDefaultRecursionDepth(source.Length, out result);
			if (hr != 0)
			{
				throw new RdcException("Failed to compute the recursion depth.", hr);
			}
			return result;
		}

		private IRdcGeneratorParameters[] InitializeGeneratorParameterses()
		{
			var result = new IRdcGeneratorParameters[_recursionDepth];
			for (var i = 0; i < _recursionDepth; i++)
			{
				_rdcLibrary.CreateGeneratorParameters(GeneratorParametersType.FilterMax, (uint)i + 1, out result[i]);
				var maxParams = (IRdcGeneratorFilterMaxParameters)result[i];

				// Set the default properties
				maxParams.SetHashWindowSize(i == 0 ? Msrdc.DefaultHashwindowsize1 : Msrdc.DefaultHashwindowsizeN);
				maxParams.SetHorizonSize(i == 0 ? Msrdc.DefaultHorizonsize1 : Msrdc.DefaultHorizonsizeN);
			}
			return result;
		}

		~SigGenerator()
		{
			try
			{
				Trace.WriteLine("~SigGenerator: Disposing esent resources from finalizer! You should call Dispose() instead!");
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
			{
				Marshal.ReleaseComObject(_rdcLibrary);
			}
		}
	}
}
