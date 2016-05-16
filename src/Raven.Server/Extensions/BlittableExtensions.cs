using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server
{
    public static class BlittableExtensions
    {
        private static readonly byte[] conversionBuffer = new byte[16];

        /// <summary>
        /// Extract enumerable of change vector from document's metadata
        /// </summary>
        /// <exception cref="InvalidDataException">Invalid data is encountered in the change vector.</exception>
        public static IEnumerable<Tuple<Guid, long>> EnumerateChangeVector(this BlittableJsonReaderObject document)
        {
            BlittableJsonReaderObject metadata;
            BlittableJsonReaderArray changeVector;
            if (document.TryGet(Constants.Metadata, out metadata) == false ||
                metadata.TryGet(Constants.DocumentReplication.DocumentChangeVector,
                out changeVector) == false)
            {
                yield break;
            }

            for (int inx = 0; inx < changeVector.Length; inx++)
            {
                if (changeVector[inx] == null)
                    throw new InvalidDataException($"Encountered invalid data in change vector. Expected BlittableJsonReaderObject, but found null");

                var vectorEntry = changeVector[inx] as BlittableJsonReaderObject;
                if(vectorEntry == null)
                    throw new InvalidDataException($"Encountered invalid data in change vector. Expected BlittableJsonReaderObject, but found {changeVector[inx].GetType()}");

                var key = vectorEntry.GetPropertyByIndex(0).Item1.AllocatedMemoryData.Address;
                var value = vectorEntry.GetPropertyByIndex(1).Item2;
            }

            throw new NotImplementedException();
        }

        private static unsafe Guid PtrToGuid(byte* ptr)
        {		    
            fixed (byte* bufferPtr = conversionBuffer)
            {
                const int GuidSize = 16;
                Memory.Copy(bufferPtr,ptr, GuidSize);
                return new Guid(conversionBuffer);
            }
        }
    }
}
