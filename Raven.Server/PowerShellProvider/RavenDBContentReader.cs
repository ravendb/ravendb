using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Management.Automation.Provider;
using Raven.Database;
using Newtonsoft.Json.Linq;

namespace Raven.Server.PowerShellProvider
{
    public class RavenDBContentReader : IContentReader
    {
        private bool _typeIsDocument;
        private int _currentOffset;
        private DocumentDatabase _db;

        public RavenDBContentReader(DocumentDatabase db, bool typeIsDocument)
        {
            this._typeIsDocument = typeIsDocument;
            this._db = db;
        }

        #region IContentReader Members

        public void Close()
        {
            Dispose();
        }

        public System.Collections.IList Read(long readCount)
        {
            JArray retVal;
            if (this._typeIsDocument)
                retVal = this._db.GetDocuments(_currentOffset, Convert.ToInt32(readCount));
            else
                retVal = this._db.GetIndexes(_currentOffset, Convert.ToInt32(readCount));

            if (retVal == null || retVal.Count == 0)
                return null;

            this._currentOffset += retVal.Count;
            return retVal.ToList();
        }

        public void Seek(long offset, System.IO.SeekOrigin origin)
        {
            int totalCount;
            if (this._typeIsDocument)
                totalCount = this._db.Statistics.CountOfDocuments;
            else
                totalCount = this._db.Statistics.CountOfIndexes;

               

            if (offset > totalCount)
            {
                throw new
                       ArgumentException(
                           "Offset cannot be greater than the number of " + (_typeIsDocument ? "documents" : "indexes")
                                        );
            }

            if (origin == System.IO.SeekOrigin.Begin)
            {
                // starting from Beginning with an index 0, the current offset
                // has to be advanced to offset - 1
                _currentOffset = _currentOffset - 1;
            }
            else if (origin == System.IO.SeekOrigin.End)
            {
                // starting from the end which is numRows - 1, the current
                // offset is so much less than numRows - 1
                _currentOffset = totalCount - 1 - _currentOffset;
            }
            else
            {
                // calculate from the previous value of current offset
                // advancing forward always
                _currentOffset += Convert.ToInt32(offset);
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            
        }

        #endregion
    }
}
