using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public class IndexFieldsPersistence
    {
        private bool _initialized;

        private readonly Index _index;

        private HashSet<string> _timeFields;

        private HashSet<string> _timeFieldsToWrite;

        public IndexFieldsPersistence(Index index)
        {
            _index = index ?? throw new ArgumentNullException(nameof(index));
        }

        internal void Initialize()
        {
            if (_initialized)
                throw new InvalidOperationException();

            _initialized = true;
            _timeFields = _index._indexStorage.ReadIndexFields();
        }

        internal void MarkHasTimeValue(string fieldName)
        {
            if (_timeFields.Contains(fieldName))
                return;

            if (_timeFieldsToWrite == null)
                _timeFieldsToWrite = new HashSet<string>();

            _timeFieldsToWrite.Add(fieldName);
        }

        internal bool HasTimeValues(string fieldName)
        {
            return _timeFields.Contains(fieldName);
        }

        internal void Persist(TransactionOperationContext indexContext)
        {
            if (_timeFieldsToWrite == null)
                return;

            _index._indexStorage.WriteIndexFields(indexContext.Transaction, _timeFieldsToWrite);

            indexContext.Transaction.InnerTransaction.LowLevelTransaction.BeforeCommitFinalization += _ =>
            {
                var timeFields = new HashSet<string>(_timeFields);
                foreach (var fieldName in _timeFieldsToWrite)
                    timeFields.Add(fieldName);

                _timeFields = timeFields;
                _timeFieldsToWrite = null;
            };
        }
    }
}
