using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.Documents.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Voron;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class DocumentTransaction : RavenTransaction
    {
        private readonly DateTime createdAt = SystemTime.UtcNow;

        private readonly List<DocumentsTask> _tasks;

        private readonly DocumentsOperationContext _context;
        private List<DocumentChangeNotification> _docChangesChangeNotifications;

        private readonly TasksStorage _tasksStorage;

        public DocumentTransaction(DocumentsOperationContext context, Transaction transaction, TasksStorage tasksStorage) 
            : base(transaction)
        {
            _context = context;
            _tasksStorage = tasksStorage;

            if (InnerTransaction.LowLevelTransaction.Flags == TransactionFlags.ReadWrite)
                _tasks = new List<DocumentsTask>();
        }

        public override void Commit()
        {
            if (InnerTransaction.LowLevelTransaction.Flags == TransactionFlags.ReadWrite)
                SaveTasks();

            base.Commit();

            if (_docChangesChangeNotifications != null)
            {
                foreach (var docChangesChangeNotification in _docChangesChangeNotifications)
                {
                    _context.DocumentDatabase.Notifications.RaiseNotifications(docChangesChangeNotification);
        }
            }
        }

        public T GetOrAddTask<T>(Func<T, bool> predicate, Func<T> newTask)
            where T : DocumentsTask
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

        public override void Dispose()
        {
            if (_context.Transaction != this)
                throw new InvalidOperationException("There is a different transaction in context.");

            _context.Transaction = null;
            base.Dispose();
        }

        private void SaveTasks()
        {
            foreach (var task in _tasks)
                _tasksStorage.AddTask(_context, task, createdAt);
        }

        public void RegisterNotification(DocumentChangeNotification documentChangeNotification)
        {
            if(_docChangesChangeNotifications == null)
                _docChangesChangeNotifications = new List<DocumentChangeNotification>();
            _docChangesChangeNotifications.Add(documentChangeNotification);
    }
    }
}