using System;
using System.Collections.Generic;
using Acornima;
using Acornima.Ast;
using Raven.Server.Utils;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch;

/// <summary>
/// A patch script made only of updates of top level fields, with values from the arguments or from literals:
/// <c>this.F = v;</c> <c>this.F += v;</c> <c>this.F -= v;</c> <c>this.F++;</c> <c>this.F--;</c> <c>this.F = this.F + v;</c> <c>this.F = this.F - v;</c>
/// where the field is <c>this.F</c>, <c>this['F']</c> or <c>this[args.f]</c>, and v is <c>args.x</c> or a literal.
/// Such a script needs no JavaScript engine, so we can build the patched document outside the transaction merger.
/// The result follows the script engine: JavaScript number arithmetic, and <see cref="JsBlittableBridge"/>'s rule for
/// writing a number back as an integer or a double. Anything else (other expressions, objects and arrays, missing or
/// mixed-type operands, '@' fields) is left to the script runner.
/// </summary>
internal sealed class SimpleAssignmentPatch
{
    private static readonly SizeLimitedConcurrentDictionary<string, SimpleAssignmentPatch> Cache = new(size: 1024, StringComparer.Ordinal);
    private static readonly SimpleAssignmentPatch NotSimple = new([]);
    private static readonly ParserOptions ParserOptions = ParserOptions.Default;

    private enum UpdateKind
    {
        Assign,
        Add,
        Subtract
    }

    // the field is either a name or the name of the argument that holds it
    private readonly record struct Target(string Field, string FieldArgument);

    // the value is either an argument or a literal (string or double)
    private readonly record struct Operand(string Argument, object Literal);

    private readonly record struct Update(Target Target, UpdateKind Kind, Operand Value);

    private static readonly Operand One = new(null, 1d);

    private readonly Update[] _updates;

    private SimpleAssignmentPatch(Update[] updates)
    {
        _updates = updates;
    }

    public static SimpleAssignmentPatch TryGet(PatchRequest patch)
    {
        if (patch?.Script == null || patch.Type != PatchRequestType.Patch || patch.HasFunctions)
            return null;

        if (Cache.TryGetValue(patch.Script, out var simple) == false)
        {
            simple = Parse(patch.Script) ?? NotSimple;
            Cache.Set(patch.Script, simple);
        }

        return ReferenceEquals(simple, NotSimple) ? null : simple;
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

        return updates.Count == 0 ? null : new SimpleAssignmentPatch(updates.ToArray());
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
            default:
                operand = default;
                return false;
        }
    }

    /// <summary>
    /// Applies the updates to <paramref name="document"/>. Returns false when the values do not fit (the caller then runs
    /// the script). <paramref name="modified"/> is null when the result equals the document.
    /// </summary>
    public bool TryApply(JsonOperationContext context, string id, BlittableJsonReaderObject document, BlittableJsonReaderObject args, out BlittableJsonReaderObject modified)
    {
        modified = null;
        if (args == null)
            return false;

        // the JavaScript value of every updated field: null, string, bool or double
        var values = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (var update in _updates)
        {
            if (TryGetField(update.Target, args, out var field) == false ||
                TryGetOperand(update.Value, args, out var operand) == false)
                return false;

            if (update.Kind == UpdateKind.Assign)
            {
                values[field] = operand;
                continue;
            }

            if (values.TryGetValue(field, out var current) == false && TryGetJsValue(document, field, out current) == false)
                return false;

            object result;
            switch (current, operand)
            {
                case (double a, double b):
                    result = update.Kind == UpdateKind.Add ? a + b : a - b;
                    if (double.IsFinite((double)result) == false)
                        return false;
                    break;
                case (string a, string b) when update.Kind == UpdateKind.Add:
                    result = a + b;
                    break;
                default:
                    return false; // JavaScript's conversions (undefined, null, bool, string with number) stay with the script runner
            }
            values[field] = result;
        }

        var modifications = new DynamicJsonValue(document);
        foreach (var (field, value) in values)
        {
            // like the engine (BlittableObjectProperty), a value that equals the stored one keeps the stored representation
            if (TryGetJsValue(document, field, out var stored) && JsEquals(stored, value))
                continue;
            modifications[field] = value is double d ? ToWrittenNumber(document, field, d) : value;
        }

        if (modifications.Properties.Count == 0)
            return true; // not modified
        document.Modifications = modifications;
        BlittableJsonReaderObject patched;
        try
        {
            patched = context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }
        finally
        {
            document.Modifications = null;
        }

        try
        {
            if (DocumentCompare.IsEqualTo(document, patched, DocumentCompare.DocumentCompareOptions.MergeMetadataAndThrowOnAttachmentModificationCompareDataArchivalMetadata) == DocumentCompareResult.Equal)
            {
                patched.Dispose();
                return true;
            }
        }
        catch (InvalidOperationException)
        {
            patched.Dispose();
            return false;
        }

        modified = patched;
        return true;
    }

    private static bool TryGetField(in Target target, BlittableJsonReaderObject args, out string field)
    {
        field = target.Field;
        if (field == null)
        {
            if (args.TryGetMember(target.FieldArgument, out object name) == false)
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

        if (args.TryGetMember(operand.Argument, out object argument) == false)
        {
            value = null;
            return false; // undefined in JavaScript
        }

        return TryToJsValue(argument, out value);
    }

    private static bool TryGetJsValue(BlittableJsonReaderObject document, string field, out object value)
    {
        if (document.TryGetMember(field, out object raw) == false)
        {
            value = null;
            return false; // undefined in JavaScript
        }
        return TryToJsValue(raw, out value);
    }

    private static bool JsEquals(object a, object b) => (a, b) switch
    {
        (null, null) => true,
        (double x, double y) => x == y,
        (string x, string y) => string.Equals(x, y, StringComparison.Ordinal),
        (bool x, bool y) => x == y,
        _ => false
    };

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
            case LazyStringValue lsv:
                value = lsv.ToString();
                return true;
            case LazyCompressedStringValue lcsv:
                value = lcsv.ToString();
                return true;
            default:
                value = null;
                return false; // objects, arrays, numbers outside the double range
        }
    }

    // JsBlittableBridge.WriteNumber: keep the original property's number type, otherwise an integer if the value is integral
    private static object ToWrittenNumber(BlittableJsonReaderObject document, string field, double d)
    {
        var propertyIndex = document.GetPropertyIndex(field);
        if (propertyIndex != -1)
        {
            var property = new BlittableJsonReaderObject.PropertyDetails();
            document.GetPropertyByIndex(propertyIndex, ref property);
            var type = property.Token & BlittableJsonReaderBase.TypesMask;
            if (type == BlittableJsonToken.LazyNumber)
                return d;
        }

        if (IsDoubleType(d))
            return d;
        return (long)d;

        static bool IsDoubleType(double d)
        {
            var roundedNumber = Math.Round(d, 0);
            if (roundedNumber.AlmostEquals(d))
            {
                var digitsAfterDecimalPoint = Math.Abs(roundedNumber - d);
                if (digitsAfterDecimalPoint == 0 && Math.Abs(roundedNumber) <= long.MaxValue)
                    return false;
            }

            return true;
        }
    }
}
