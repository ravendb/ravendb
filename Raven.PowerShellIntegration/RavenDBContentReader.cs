using System;
using System.Collections;
using System.IO;
using System.Linq;
using System.Management.Automation.Provider;
using Newtonsoft.Json.Linq;
using Raven.Database;

namespace Raven.PowerShellIntegration
{
	public class RavenDBContentReader : IContentReader
	{
		private readonly DocumentDatabase _db;
		private readonly bool _typeIsDocument;
		private long _currentOffset;

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

		public IList Read(long readCount)
		{
			JArray retVal;
			if (this._typeIsDocument)
				retVal = this._db.GetDocuments((int)_currentOffset, Convert.ToInt32(readCount), null);
			else
				retVal = this._db.GetIndexes((int)_currentOffset, Convert.ToInt32(readCount));

			if (retVal == null || retVal.Count == 0)
				return null;

			this._currentOffset += retVal.Count;
			return retVal.ToList();
		}

		public void Seek(long offset, SeekOrigin origin)
		{
			long totalCount;
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

			if (origin == SeekOrigin.Begin)
			{
				// starting from Beginning with an index 0, the current offset
				// has to be advanced to offset - 1
				_currentOffset = _currentOffset - 1;
			}
			else if (origin == SeekOrigin.End)
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

		public void Dispose()
		{
		}

		#endregion
	}
}
