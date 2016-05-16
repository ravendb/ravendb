using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        /// <summary>
        /// Extract enumerable of change vector from document's metadata
        /// </summary>
        /// <exception cref="InvalidDataException">Invalid data is encountered in the change vector.</exception>
        public static IEnumerable<Tuple<BlittableJsonReaderArray, long>> EnumerateChangeVector(this BlittableJsonReaderObject document)
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
                    throw new InvalidDataException("Encountered invalid data in change vector. Expected BlittableJsonReaderObject, but found null");

                var vectorEntry = changeVector[inx] as BlittableJsonReaderObject;
                if(vectorEntry == null)
                    throw new InvalidDataException($"Encountered invalid data in change vector. Expected BlittableJsonReaderObject, but found {changeVector[inx].GetType()}");

                var key = vectorEntry.GetPropertyByIndex(0);
                if(key.Item3.HasFlag(BlittableJsonToken.StartArray) == false)
                    throw new InvalidDataException($"Encountered invalid data in extracting document change vector. Expected a json array, but found {key.Item3}");

                var val = vectorEntry.GetPropertyByIndex(1);
                if(val.Item3 != BlittableJsonToken.Integer)
                    throw new InvalidDataException($"Encountered invalid data in extracting document change vector. Expected a number, but found {key.Item3}");

                var byteArray = key.Item2 as BlittableJsonReaderArray;
                yield return Tuple.Create(byteArray, (long)val.Item2);
            }

        }
    }
}
