using System;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Operations
{
    public partial class OperationExecuter
    {
        private readonly DocumentStoreBase _store;
        private readonly string _databaseName;
        private readonly RequestExecuter _requestExecuter;
        private readonly JsonOperationContext _context;

        public OperationExecuter(DocumentStoreBase store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.DefaultDatabase;
            _requestExecuter = store.GetRequestExecuter(databaseName);
        }

        internal OperationExecuter(DocumentStoreBase store, RequestExecuter requestExecuter, JsonOperationContext context)
        {
            _store = store;
            _requestExecuter = requestExecuter;
            _context = context;
        }

        public OperationExecuter ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new OperationExecuter(_store, databaseName);
        }

        private IDisposable GetContext(out JsonOperationContext context)
        {
            if (_context == null)
                return _requestExecuter.ContextPool.AllocateOperationContext(out context);

            context = _context;
            return null;
        }
    }
}