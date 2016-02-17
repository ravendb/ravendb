using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions;
using Raven.Client.Data;
using Raven.Server.Documents.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Voron;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class DocumentsTransaction : RavenTransaction
    {
        private readonly DateTime createdAt = SystemTime.UtcNow;

        private readonly List<DocumentsTask> _tasks;

        private readonly DocumentsOperationContext _context;

        private readonly TasksStorage _tasksStorage;

        private readonly DocumentsNotifications _notifications;

        private readonly List<Notification> _afterCommitNotifications = new List<Notification>();

        public DocumentsTransaction(DocumentsOperationContext context, Transaction transaction, TasksStorage tasksStorage, DocumentsNotifications notifications)
            : base(transaction)
        {
            _context = context;
            _tasksStorage = tasksStorage;
            _notifications = notifications;

            if (InnerTransaction.LowLevelTransaction.Flags == TransactionFlags.ReadWrite)
                _tasks = new List<DocumentsTask>();
        }

        public override void Commit()
        {
            if (InnerTransaction.LowLevelTransaction.Flags == TransactionFlags.ReadWrite)
                SaveTasks();

            base.Commit();

            AfterCommit();
        }

        public void AddAfterCommitNotification(Notification notification)
        {
            _afterCommitNotifications.Add(notification);
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

        private void AfterCommit()
        {
            foreach (var notification in _afterCommitNotifications)
                _notifications.RaiseNotifications(notification);
        }

        private void SaveTasks()
        {
            foreach (var task in _tasks)
                _tasksStorage.AddTask(_context, task, createdAt);
        }
    }
}