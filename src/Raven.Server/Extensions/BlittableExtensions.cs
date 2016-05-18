using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Server.Documents.Replication;
using Sparrow.Json;

namespace Raven.Server
{
    public static class BlittableExtensions
    {
        /// <summary>
        /// Extract enumerable of change vector from document's metadata
        /// </summary>
        /// <exception cref="InvalidDataException">Invalid data is encountered in the change vector.</exception>        
        public static ChangeVectorEntry[] EnumerateChangeVector(this BlittableJsonReaderObject document)
        {
            //TODO: do not forget to investigate a bug in here
            //(last result in the vector key seems corrupted)
            BlittableJsonReaderObject metadata;
            BlittableJsonReaderArray changeVector;
            if (document.TryGet(Constants.Metadata, out metadata) == false ||
                metadata.TryGet(Constants.DocumentReplication.DocumentChangeVector,
                out changeVector) == false)
            {
                return new ChangeVectorEntry[0];
            }

            var results = new ChangeVectorEntry[changeVector.Length];

            for (int inx = 0; inx < changeVector.Length; inx++)
            {
                if (changeVector[inx] == null)
                    throw new InvalidDataException("Encountered invalid data in change vector. Expected BlittableJsonReaderObject, but found null");

                var vectorEntry = changeVector[inx] as BlittableJsonReaderObject;
                if(vectorEntry == null)
                    throw new InvalidDataException($"Encountered invalid data in change vector. Expected BlittableJsonReaderObject, but found {changeVector[inx].GetType()}");

                var key = vectorEntry.GetPropertyByIndex(0);
                if(key.Item3 != BlittableJsonToken.String)
                    throw new InvalidDataException($"Encountered invalid data in extracting document change vector. Expected a string, but found {key.Item3}");

                var val = vectorEntry.GetPropertyByIndex(1);
                if(val.Item3 != BlittableJsonToken.Integer)
                    throw new InvalidDataException($"Encountered invalid data in extracting document change vector. Expected a number, but found {key.Item3}");

                results[inx] = new ChangeVectorEntry
                {
                    DbId = Guid.Parse(key.Item2.ToString()),
                    Etag = (long)val.Item2
                };
            }

            return results;
        }
    }
}
