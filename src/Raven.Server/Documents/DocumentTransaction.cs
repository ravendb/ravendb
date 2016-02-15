using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions;
using Raven.Server.Documents.Tasks;
using Raven.Server.Json;

using Voron;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class DocumentTransaction : IDisposable
    {
        private readonly DateTime createdAt = SystemTime.UtcNow;

        private readonly List<DocumentTask> _tasks;

        private readonly RavenOperationContext _context;

        public readonly Transaction InnerTransaction;

        public DocumentTransaction(RavenOperationContext context, Transaction transaction)
        {
            _context = context;
            InnerTransaction = transaction;

            if (InnerTransaction.LowLevelTransaction.Flags == TransactionFlags.ReadWrite)
                _tasks = new List<DocumentTask>();
        }

        public void Commit()
        {
            if (InnerTransaction.LowLevelTransaction.Flags == TransactionFlags.ReadWrite)
                SaveTasks();

            InnerTransaction.Commit();
        }

        public T GetTask<T>(Func<T, bool> predicate, Func<T> newTask)
            where T : DocumentTask
        {
            var task = _tasks
                .OfType<T>()
                .FirstOrDefault(predicate);

            if (task != null)
                return task;

            var t = newTask();
            _tasks.Add(t);
            return t;
        }

        public void Dispose()
        {
            if (_context.Transaction != this)
                throw new InvalidOperationException("There is a different transaction in context.");

            _context.Transaction = null;
            InnerTransaction?.Dispose();
        }

        private void SaveTasks()
        {
            foreach (var task in _tasks)
            {
                Tasks.AddTask(task, createdAt);
            }
        }
    }
}