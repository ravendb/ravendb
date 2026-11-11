using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Acornima;
using Acornima.Ast;
using Sparrow;
using Sparrow.Json;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Patch;

/// <summary>
/// A patch script made only of updates of top level fields, with values from the arguments or from literals:
/// <c>this.F = v;</c> <c>this.F += v;</c> <c>this.F -= v;</c> <c>this.F++;</c> <c>this.F--;</c> <c>this.F = this.F + v;</c> <c>this.F = this.F - v;</c>
/// where the field is <c>this.F</c>, <c>this['F']</c> or <c>this[args.f]</c>, and v is <c>args.x</c> or a literal (string, number, boolean, null).
/// Such a script needs no JavaScript engine, so we can build the patched document outside the transaction merger.
/// The result follows the script engine: JavaScript number arithmetic, and <see cref="JsBlittableBridge"/>'s rule for
/// writing a number back as an integer or a double. Anything else (other expressions, objects and arrays, missing or
/// mixed-type operands, '@' fields) is left to the script runner.
/// </summary>
internal sealed class SimpleAssignmentPatch : ILowMemoryHandler
{
    private static readonly SimpleAssignmentPatch Scripts = new([], new ScriptCache(size: 1024)); // the notification holds its handlers weakly
    private static readonly SimpleAssignmentPatch NotSimple = new([]);
    private static readonly ParserOptions ParserOptions = ParserOptions.Default;

    static SimpleAssignmentPatch()
    {
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(Scripts);
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity) => _cache?.Clear();

    public void LowMemoryOver()
    {
    }

    internal static bool IsCached(string script) => Scripts._cache.Contains(script);

    internal static void LowMemoryForTesting() => Scripts.LowMemory(LowMemorySeverity.ExtremelyLow);

    private enum UpdateKind
    {
        Assign,
        Add,
        Subtract
    }

    // the field is either a name or the name of the argument that holds it
    private readonly record struct Target(string Field, string FieldArgument);

    // the value is either an argument or a literal (string, double, bool or null)
    private readonly record struct Operand(string Argument, object Literal);

    private readonly record struct Update(Target Target, UpdateKind Kind, Operand Value);

    private const int MaxUpdates = 16;

    private static readonly Operand One = new(null, 1d);

    private readonly Update[] _updates;
    private readonly ScriptCache _cache; // the parsed scripts, only on Scripts

    private SimpleAssignmentPatch(Update[] updates, ScriptCache cache = null)
    {
        _updates = updates;
        _cache = cache;
    }

    public static SimpleAssignmentPatch TryGet(PatchRequest patch)
    {
        if (patch?.Script == null || patch.Type != PatchRequestType.Patch || patch.HasFunctions || MayBeSimple(patch.Script) == false)
            return null;

        if (Scripts._cache.TryGet(patch.Script, out var simple) == false)
            simple = Scripts._cache.Add(patch.Script, Parse(patch.Script) ?? NotSimple);

        return ReferenceEquals(simple, NotSimple) ? null : simple;
    }

    private const int MaxScriptLength = 1024;
    private static readonly SearchValues<char> NeverInASimpleScript = SearchValues.Create("({?");

    
    private static bool MayBeSimple(string script) => // a simple script is short and has no call, block, object or conditional
        script.Length <= MaxScriptLength && script.AsSpan().IndexOfAny(NeverInASimpleScript) < 0;

    /// <summary>
    /// The parsed scripts, shared by all the databases. Second-chance eviction: a script used since the eviction last passed
    /// it gets another round, so the scripts in use stay while one-off scripts go. Concurrent misses add a script once.
    /// </summary>
    internal sealed class ScriptCache(int size)
    {
        private sealed class Entry(SimpleAssignmentPatch value)
        {
            public readonly SimpleAssignmentPatch Value = value;
            public volatile bool Used;
        }

        private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
        private readonly ConcurrentQueue<string> _order = new();

        public bool Contains(string script) => _entries.ContainsKey(script);

        internal int Count => _entries.Count;

        internal int QueuedForEviction => _order.Count;

        public bool TryGet(string script, out SimpleAssignmentPatch value)
        {
            if (_entries.TryGetValue(script, out var entry) == false)
            {
                value = null;
                return false;
            }

            if (entry.Used == false)
                entry.Used = true; // written once, so we don't dirty the CPU cache line each time
            value = entry.Value;
            return true;
        }

        public SimpleAssignmentPatch Add(string script, SimpleAssignmentPatch value)
        {
            var entry = new Entry(value);
            var current = _entries.GetOrAdd(script, entry);
            if (ReferenceEquals(current, entry) == false)
                return current.Value; // another thread added it

            if (_order.Count >= size)
                EvictOne();
            _order.Enqueue(script); // after the eviction, so the new script is not the one evicted
            return value;
        }

        // Approximate under concurrent adds. Two rounds at most: the first clears the used marks, so the second finds a script.
        private void EvictOne()
        {
            for (var i = 0; i < 2 * size && _order.TryDequeue(out var oldest); i++)
            {
                if (_entries.TryGetValue(oldest, out var candidate) == false)
                    continue;
                if (candidate.Used)
                {
                    candidate.Used = false;
                    _order.Enqueue(oldest);
                    continue;
                }
                if (_entries.TryRemove(oldest, out _))
                    return;
            }
        }

        public void Clear()
        {
            _entries.Clear();
            _order.Clear();
        }
    }

    private static SimpleAssignmentPatch Parse(string script)
    {
        Script program;
        try
        {
            program = new Parser(ParserOptions).ParseScript(script);
        }
        catch (ParseErrorException)
        {
            return null; // the script runner reports the error
        }

        var updates = new List<Update>();
        foreach (var statement in program.Body)
        {
            if (statement is EmptyStatement)
                continue;
            if (statement is not ExpressionStatement { Expression: var expression } || TryParseUpdate(expression, out var update) == false)
                return null;
            updates.Add(update);
        }

        if (updates.Count == 0 || updates.Count > MaxUpdates)
            return null;

        if (updates.Count > 1)
        {
            // each update has a field of its own; a field from the arguments may be any of them
            var fields = new HashSet<string>(StringComparer.Ordinal);
            foreach (var update in updates)
            {
                if (update.Target.Field == null || fields.Add(update.Target.Field) == false)
                    return null;
            }
        }

        return new SimpleAssignmentPatch(updates.ToArray());
    }

    private static bool TryParseUpdate(Expression expression, out Update update)
    {
        update = default;
        switch (expression)
        {
            case UpdateExpression { Operator: Operator.Increment or Operator.Decrement } increment when TryParseTarget(increment.Argument, out var target):
                // as a statement, prefix and postfix forms have the same effect
                update = new Update(target, increment.Operator == Operator.Increment ? UpdateKind.Add : UpdateKind.Subtract, One);
                return true;

            case AssignmentExpression assignment when TryParseTarget(assignment.Left, out var target):
                switch (assignment.Operator)
                {
                    case Operator.Assignment when TryParseOperand(assignment.Right, out var value):
                        update = new Update(target, UpdateKind.Assign, value);
                        return true;
                    case Operator.Assignment when assignment.Right is NonLogicalBinaryExpression { Operator: Operator.Addition or Operator.Subtraction } binary &&
                                                  TryParseTarget(binary.Left, out var self) && self == target && TryParseOperand(binary.Right, out var value):
                        update = new Update(target, binary.Operator == Operator.Addition ? UpdateKind.Add : UpdateKind.Subtract, value);
                        return true;
                    case Operator.AdditionAssignment when TryParseOperand(assignment.Right, out var value):
                        update = new Update(target, UpdateKind.Add, value);
                        return true;
                    case Operator.SubtractionAssignment when TryParseOperand(assignment.Right, out var value):
                        update = new Update(target, UpdateKind.Subtract, value);
                        return true;
                }
                return false;

            default:
                return false;
        }
    }

    private static bool TryParseTarget(Node node, out Target target)
    {
        target = default;
        if (node is not MemberExpression { Object: ThisExpression } member)
            return false;

        switch (member.Property)
        {
            case Identifier name when member.Computed == false:
                target = new Target(name.Name, null);
                return true;
            case StringLiteral name when member.Computed:
                target = new Target(name.Value, null);
                return true;
            case MemberExpression { Object: Identifier { Name: "args" }, Computed: false, Property: Identifier argument } when member.Computed:
                target = new Target(null, argument.Name);
                return true;
            default:
                return false;
        }
    }

    private static bool TryParseOperand(Node node, out Operand operand)
    {
        switch (node)
        {
            case MemberExpression { Object: Identifier { Name: "args" }, Computed: false, Property: Identifier argument }:
                operand = new Operand(argument.Name, null);
                return true;
            case MemberExpression { Object: Identifier { Name: "args" }, Computed: true, Property: StringLiteral argument }:
                operand = new Operand(argument.Value, null);
                return true;
            case NumericLiteral number:
                operand = new Operand(null, number.Value);
                return true;
            case StringLiteral text:
                operand = new Operand(null, text.Value);
                return true;
            case BooleanLiteral boolean:
                operand = new Operand(null, boolean.Value);
                return true;
            case NullLiteral:
                operand = new Operand(null, null);
                return true;
            default:
                operand = default;
                return false;
        }
    }

    // A field the script updates: where it is in the document, the value the script engine sees there, and its new value
    private struct Change
    {
        public string Field;
        public int PropertyIndex; // -1: a new property
        public int Offset;        // of the stored value: insertion order is descending offset
        public BlittableJsonToken OriginalType;
        public bool HasStored;    // false: undefined in JavaScript
        public object Stored;     // null, bool, double, or a string: LazyStringValue, LazyCompressedStringValue or string
        public object Value;      // like Stored
        public bool Unchanged;
    }

    [InlineArray(MaxUpdates)]
    private struct Changes
    {
        private Change _first;
    }

    /// <summary>
    /// Applies the updates to <paramref name="document"/>. Returns false when the values do not fit (the caller then runs
    /// the script). <paramref name="modified"/> is null when the result equals the document.
    /// </summary>
    public bool TryApply(JsonOperationContext context, BlittableJsonReaderObject document, BlittableJsonReaderObject args, out BlittableJsonReaderObject modified)
    {
        modified = null;
        var buffer = new Changes();
        Span<Change> changes = buffer[.._updates.Length];
        for (var i = 0; i < _updates.Length; i++)
        {
            var update = _updates[i];
            if (TryGetField(update.Target, args, out var field) == false ||
                TryGetOperand(update.Value, args, out var operand) == false)
                return false;

            ref var change = ref changes[i];
            change = Find(document, field);
            if (update.Kind == UpdateKind.Assign)
            {
                change.Value = operand;
                continue;
            }

            switch (change.Stored, operand)
            {
                case (double a, double b):
                    var result = update.Kind == UpdateKind.Add ? a + b : a - b;
                    if (double.IsFinite(result) == false)
                        return false;
                    change.Value = result;
                    break;
                case var (a, b) when update.Kind == UpdateKind.Add && IsString(a) && IsString(b):
                    change.Value = a.ToString() + b.ToString();
                    break;
                default:
                    return false; // JavaScript's conversions (undefined, null, bool, string with number) stay with the script runner
            }
        }

        // like the engine (BlittableObjectProperty), a value that equals the stored one keeps the stored representation
        var changed = 0;
        foreach (ref var change in changes)
        {
            change.Unchanged = change.HasStored && JsEquals(change.Stored, change.Value);
            if (change.Unchanged == false)
                changed++;
        }

        if (changed == 0)
            return true; // not modified

        modified = Write(context, document, changes);
        return true;
    }

    private static Change Find(BlittableJsonReaderObject document, string field)
    {
        var change = new Change { Field = field, PropertyIndex = document.GetPropertyIndex(field), OriginalType = BlittableJsonToken.Null };
        if (change.PropertyIndex == -1)
            return change;

        change.Offset = document.GetPropertyOffset(change.PropertyIndex);
        var property = new BlittableJsonReaderObject.PropertyDetails();
        document.GetPropertyByIndex(change.PropertyIndex, ref property);
        change.OriginalType = property.Token & BlittableJsonReaderBase.TypesMask;
        change.HasStored = TryToJsValue(property.Value, out change.Stored);
        return change;
    }

    // Writes the document as the script engine does: the properties in their order, a changed one in its place and a new one last
    private static unsafe BlittableJsonReaderObject Write(JsonOperationContext context, BlittableJsonReaderObject document, ReadOnlySpan<Change> changes)
    {
        using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
        {
            context.CachedProperties.NewDocument(); // the context may have written other documents: our names table holds only ours
            writer.Reset(BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            writer.StartWriteObjectDocument();
            writer.StartWriteObject();

            // the changes of existing properties, in insertion order
            Span<int> offsets = stackalloc int[MaxUpdates];
            Span<int> inPlace = stackalloc int[MaxUpdates];
            var count = 0;
            for (var c = 0; c < changes.Length; c++)
            {
                if (changes[c].PropertyIndex == -1 || changes[c].Unchanged)
                    continue;
                offsets[count] = -changes[c].Offset;
                inPlace[count++] = c;
            }
            offsets[..count].Sort(inPlace[..count]);

            var next = 0;
            var property = new BlittableJsonReaderObject.PropertyDetails();
            using (var order = document.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < order.Size; i++)
                {
                    var index = order.Properties[i];
                    document.GetPropertyByIndex(index, ref property);
                    writer.WritePropertyName(property.Name);
                    if (next < count && changes[inPlace[next]].PropertyIndex == index)
                        WriteValue(writer, changes[inPlace[next++]]);
                    else
                        writer.WriteValue(property.Token & BlittableJsonReaderBase.TypesMask, property.Value);
                }
            }

            foreach (ref readonly var change in changes)
            {
                if (change.PropertyIndex != -1 || change.Unchanged)
                    continue;
                writer.WritePropertyName(change.Field);
                WriteValue(writer, change);
            }

            writer.WriteObjectEnd();
            writer.FinalizeDocument();
            return writer.CreateReader();
        }
    }

    private static void WriteValue(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, in Change change)
    {
        switch (change.Value)
        {
            case null:
                writer.WriteValueNull();
                break;
            case string s:
                writer.WriteValue(s);
                break;
            case LazyStringValue lsv:
                writer.WriteValue(lsv.AsReadOnlySpan());
                break;
            case LazyCompressedStringValue lcsv:
                writer.WriteValue(lcsv);
                break;
            case bool b:
                writer.WriteValue(b);
                break;
            case double d when JsBlittableBridge.WriteAsDouble(change.OriginalType, d):
                writer.WriteValue(d);
                break;
            case double d:
                writer.WriteValue((long)d);
                break;
            default:
                throw new InvalidOperationException($"Unexpected value of type {change.Value.GetType()} for '{change.Field}'");
        }
    }

    private static bool TryGetField(in Target target, BlittableJsonReaderObject args, out string field)
    {
        field = target.Field;
        if (field == null)
        {
            if (args == null || args.TryGetMember(target.FieldArgument, out object name) == false)
                return false;
            field = name switch
            {
                LazyStringValue lsv => lsv.ToString(),
                LazyCompressedStringValue lcsv => lcsv.ToString(),
                _ => null
            };
        }

        // '@metadata' and the other '@' properties carry server semantics, leave them to the script runner
        return string.IsNullOrEmpty(field) == false && field[0] != '@';
    }

    private static bool TryGetOperand(in Operand operand, BlittableJsonReaderObject args, out object value)
    {
        if (operand.Argument == null)
        {
            value = operand.Literal;
            return true;
        }

        if (args == null || args.TryGetMember(operand.Argument, out object argument) == false)
        {
            value = null;
            return false; // undefined in JavaScript
        }

        return TryToJsValue(argument, out value);
    }

    private static bool JsEquals(object a, object b) => (a, b) switch
    {
        (null, null) => true,
        (double x, double y) => x == y,
        (bool x, bool y) => x == y,
        _ => IsString(a) && IsString(b) && StringEquals(a, b)
    };

    private static bool IsString(object value) => value is LazyStringValue or LazyCompressedStringValue or string;

    private static bool StringEquals(object a, object b)
    {
        if (a is LazyCompressedStringValue compressed)
            return CompressedEquals(compressed, b);
        if (b is LazyCompressedStringValue other)
            return CompressedEquals(other, a);

        return (a, b) switch
        {
            (LazyStringValue x, LazyStringValue y) => x.Equals(y),
            (LazyStringValue x, string y) => x.Equals(y),
            (string x, LazyStringValue y) => y.Equals(x),
            _ => string.Equals((string)a, (string)b, StringComparison.Ordinal)
        };
    }

    private static unsafe bool CompressedEquals(LazyCompressedStringValue compressed, object other)
    {
        // equal compressed bytes are equal strings; different ones may still be (another compressor version or setting)
        if (other is LazyCompressedStringValue otherCompressed && otherCompressed.CompressedSize == compressed.CompressedSize &&
            Memory.Compare(compressed.Buffer, otherCompressed.Buffer, compressed.CompressedSize) == 0)
            return true;

        var size = other switch
        {
            LazyStringValue lsv => lsv.Size,
            LazyCompressedStringValue lcsv => lcsv.UncompressedSize,
            _ => Encoding.UTF8.GetByteCount((string)other)
        };
        if (size != compressed.UncompressedSize)
            return false; // no need to decompress

        using (var uncompressed = compressed.ToLazyStringValue())
            return StringEquals(uncompressed, other);
    }

    // the value the script engine sees for a blittable value (BlittableObjectInstance), limited to the types we handle
    private static bool TryToJsValue(object raw, out object value)
    {
        switch (raw)
        {
            case null:
                value = null;
                return true;
            case bool b:
                value = b;
                return true;
            case long l:
                value = (double)l;
                return true;
            case LazyNumberValue lnv when lnv.TryParseDouble(out var d):
                value = d;
                return true;
            case LazyStringValue or LazyCompressedStringValue:
                value = raw;
                return true;
            default:
                value = null;
                return false; // objects, arrays, numbers outside the double range
        }
    }
}
