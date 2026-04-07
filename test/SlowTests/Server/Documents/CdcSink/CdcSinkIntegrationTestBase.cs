using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public abstract class CdcSinkIntegrationTestBase : SqlAwareTestBase
    {
        protected CdcSinkIntegrationTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected AddCdcSinkOperationResult AddCdcSink(IDocumentStore store, CdcSinkConfiguration config)
        {
            return store.Maintenance.Send(new AddCdcSinkOperation(config));
        }

        /// <summary>
        /// Waits for the CDC Sink's initial load phase to complete.
        /// Uses the InitialLoadCompleted TaskCompletionSource on CdcSinkProcess,
        /// which is created at construction time (no race with process start).
        /// </summary>
        protected async Task WaitForCdcInitialLoadAsync(IDocumentStore store, string configName, int timeoutMs = 60_000)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == configName);
            if (process == null)
                throw new InvalidOperationException($"CDC Sink process '{configName}' not found");

            var completed = await Task.WhenAny(process.InitialLoadCompleted, Task.Delay(timeoutMs));
            if (completed != process.InitialLoadCompleted)
                throw new TimeoutException($"CDC Sink '{configName}' initial load did not complete within {timeoutMs}ms");

            await process.InitialLoadCompleted; // propagate any exception
        }

        /// <summary>
        /// Subscribes to ProcessError on the named CDC Sink process and returns a task
        /// that completes when the next error occurs. Use this before triggering an
        /// error condition to avoid polling FallbackTime.
        /// </summary>
        protected async Task<Task<Exception>> WaitForNextProcessError(IDocumentStore store, string configName)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == configName);
            if (process == null)
                throw new InvalidOperationException($"CDC Sink process '{configName}' not found");

            var tcs = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            process.ProcessError += ex => tcs.TrySetResult(ex);
            return tcs.Task;
        }

        protected async Task<T> WaitForDocumentAsync<T>(IDocumentStore store, string docId, int timeoutMs = 30_000)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<T>(docId);
                    if (doc != null)
                        return doc;
                }

                await Task.Delay(250);
            }

            return null;
        }

        protected async Task<bool> WaitForDocumentDeletionAsync(IDocumentStore store, string docId, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<object>(docId);
                    if (doc == null)
                        return true;
                }

                await Task.Delay(250);
            }

            return false;
        }

        protected async Task<int> WaitForDocumentCountAsync(IDocumentStore store, string collectionName, int expectedCount, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            int count = 0;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using (var session = store.OpenAsyncSession())
                {
                    count = await session.Query<dynamic>(collectionName: collectionName).CountAsync();
                    if (count >= expectedCount)
                        return count;
                }

                await Task.Delay(250);
            }

            return count;
        }

        // ─────────────────────────────────────────────────────────────────────
        // Shared DTO classes
        // ─────────────────────────────────────────────────────────────────────

        protected class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public decimal Price { get; set; }
            public double TotalPrice { get; set; }
        }

        protected class Item
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        protected class Event
        {
            public string Id { get; set; }
            public string Description { get; set; }
            public string Title { get; set; }
        }

        protected class Note
        {
            public string Id { get; set; }
            public string Content { get; set; }
        }

        protected class Record
        {
            public string Id { get; set; }
            public string Title { get; set; }
        }

        protected class OrderLine
        {
            public int LineNum { get; set; }
            public string LineId { get; set; }
            public string Product { get; set; }
            public int Quantity { get; set; }
        }

        protected class Order
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public string Customer { get; set; }
            public decimal Total { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        protected class Person
        {
            public string Id { get; set; }
            public string FullName { get; set; }
        }

        protected class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
            public string InternalNotes { get; set; }
        }

        protected class Employee
        {
            public int EmpId { get; set; }
            public string EmpName { get; set; }
        }

        protected class Department
        {
            public int DeptId { get; set; }
            public string DeptName { get; set; }
            public List<Employee> Employees { get; set; }
        }

        protected class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<Department> Departments { get; set; }
        }

        protected class Counter
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Value { get; set; }
        }

        protected class InvoiceLine
        {
            public int LineNum { get; set; }
            public string Description { get; set; }
        }

        protected class Invoice
        {
            public string Id { get; set; }
            public string Customer { get; set; }
            public double DiscountPct { get; set; }
            public double LineAmount { get; set; }
            public List<InvoiceLine> Lines { get; set; }
        }

        protected class FileDoc
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        protected class TextAttachmentDoc
        {
            public int DbId { get; set; }
            public string Title { get; set; }
        }

        protected class OrderWithStatus
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public string Status { get; set; }
        }

        protected class ArchivedOrder
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public bool Archived { get; set; }
            public string ArchivedAt { get; set; }
        }

        protected class OrderWithDeleteTracking
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public List<OrderLine> Lines { get; set; }
            public int LastDeletedLine { get; set; }
            public int DeleteCount { get; set; }
            public int LastArchivedLine { get; set; }
            public int ArchiveCount { get; set; }
        }

        protected class EmployeeStringFields
        {
            public string Name { get; set; }
            public string Birthday { get; set; }
            public string Salary { get; set; }
            public string EmployeeId { get; set; }
            public bool Active { get; set; }
            public string Age { get; set; }
            public string Score { get; set; }
        }

        protected class EmployeeRecord
        {
            public int DbId { get; set; }
            public string Name { get; set; }
            public string Department { get; set; }
        }

        protected class Car
        {
            public int DbId { get; set; }
            public string Make { get; set; }
            public string Model { get; set; }
        }

        protected class Photo
        {
            public int PhotoNum { get; set; }
            public string Title { get; set; }
        }

        protected class Album
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<Photo> Photos { get; set; }
        }

        protected class InvoiceWithTotal
        {
            public string Id { get; set; }
            public string Customer { get; set; }
            public double TotalAmount { get; set; }
            public List<InvoiceLineWithAmount> Lines { get; set; }
        }

        protected class InvoiceLineWithAmount
        {
            public int LineNum { get; set; }
            public string Description { get; set; }
            public decimal Amount { get; set; }
        }

        protected class AuditEntry
        {
            public string Op { get; set; }
            public string Name { get; set; }
            public string PreviousName { get; set; }
            public string Timestamp { get; set; }
        }

        protected class ConfigDoc
        {
            public int DbId { get; set; }
            public string Name { get; set; }
            public ConfigSettings Settings { get; set; }
            public List<string> Tags { get; set; }
            public string Description { get; set; }
        }

        protected class ConfigSettings
        {
            public string Theme { get; set; }
            public NotificationSettings Notifications { get; set; }
        }

        protected class NotificationSettings
        {
            public bool Email { get; set; }
            public bool Sms { get; set; }
        }
    }
}
