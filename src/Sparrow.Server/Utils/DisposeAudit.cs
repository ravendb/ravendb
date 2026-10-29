using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Sparrow.Collections;

namespace Sparrow.Server.Utils;

public sealed class DisposeAudit
{
    private static readonly AsyncLocal<Scope> CurrentScope = new();

    private static readonly ConcurrentSet<Scope> Running = new();

    // scopes open and close concurrently (the landlord disposes databases in parallel) while a snapshot reads them
    private readonly ConcurrentSet<Scope> _active = new();

    private DisposeAudit()
    {
    }

    public static Scope Begin(string name)
    {
        var root = new Scope(new DisposeAudit(), parent: null, name);
        Running.TryAdd(root);
        return root;
    }

    public static Scope BeginOrStep(string name) => Step(name) ?? Begin(name);

    public static string SnapshotAll()
    {
        var running = Running.OrderBy(root => root.StartTimestamp).ToList();
        return running.Count == 0 ? null : string.Join(Environment.NewLine, running.Select(root => root.Snapshot()));
    }

    public static Scope Step(string name) =>
        CurrentScope.Value is { IsActive: true } parent ? new Scope(parent.Audit, parent, name) : null;

    public static void Progress(string message)
    {
        if (CurrentScope.Value is { IsActive: true } scope)
            scope.SetProgress(message);
    }

    public sealed class Scope : IDisposable
    {
        internal readonly DisposeAudit Audit;
        private readonly Scope _parent;
        private readonly string _name;
        internal readonly long StartTimestamp;
        private volatile ProgressMark _progress;
        private volatile bool _disposed;

        private sealed record ProgressMark(string Message, long Timestamp);

        internal Scope(DisposeAudit audit, Scope parent, string name)
        {
            Audit = audit;
            _parent = parent;
            _name = name;
            StartTimestamp = Stopwatch.GetTimestamp();

            audit._active.TryAdd(this);
            CurrentScope.Value = this;
        }

        // a flow can outlive the scope it started in (a deferred continuation), and must not nest under it then
        internal bool IsActive => _disposed == false;

        internal void SetProgress(string message) => _progress = new ProgressMark(message, Stopwatch.GetTimestamp());

        public string Snapshot()
        {
            var sb = new StringBuilder($"Dispose of '{_name}' {(IsActive ? $"running for {Stopwatch.GetElapsedTime(StartTimestamp)}:" : "completed.")}");

            var active = Audit._active.ToList();
            foreach (var leaf in active.Where(scope => active.Any(other => other._parent == scope) == false).OrderBy(scope => scope.StartTimestamp))
            {
                sb.AppendLine();
                sb.Append("    ");
                leaf.DescribeChain(sb);
            }

            return sb.ToString();
        }

        private void DescribeChain(StringBuilder sb)
        {
            if (_parent != null)
            {
                _parent.DescribeChain(sb);
                sb.Append(" > ");
            }

            sb.Append($"{_name} ({Stopwatch.GetElapsedTime(StartTimestamp)})");

            if (_progress is { } progress)
                sb.Append($" [at '{progress.Message}' since {Stopwatch.GetElapsedTime(progress.Timestamp)}]");
        }

        public void Dispose()
        {
            _disposed = true;
            Audit._active.TryRemove(this);

            if (_parent == null)
                Running.TryRemove(this);

            if (CurrentScope.Value == this)
                CurrentScope.Value = _parent is { IsActive: true } ? _parent : null;
        }
    }
}
