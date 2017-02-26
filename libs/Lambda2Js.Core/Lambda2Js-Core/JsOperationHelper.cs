using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Lambda2Js
{
    public static class JsOperationHelper
    {
        private static readonly ExpressionType[] post = new[]
            {
                ExpressionType.ArrayLength,
                ExpressionType.PostIncrementAssign,
                ExpressionType.PostDecrementAssign,
            };

        public static bool IsPostfixOperator(ExpressionType op)
        {
            return post.Contains(op);
        }

        //private static readonly ExpressionType[] assign = new[]
        //    {
        //        ExpressionType.Assign,
        //        ExpressionType.AddAssign,
        //        ExpressionType.AddAssignChecked,
        //        ExpressionType.AndAssign,
        //        ExpressionType.DivideAssign,
        //        ExpressionType.ExclusiveOrAssign,
        //        ExpressionType.LeftShiftAssign,
        //        ExpressionType.ModuloAssign,
        //        ExpressionType.MultiplyAssign,
        //        ExpressionType.MultiplyAssignChecked,
        //        ExpressionType.OrAssign,
        //        ExpressionType.PostDecrementAssign,
        //        ExpressionType.PostIncrementAssign,
        //        ExpressionType.PowerAssign,
        //        ExpressionType.PreDecrementAssign,
        //        ExpressionType.PreIncrementAssign,
        //        ExpressionType.RightShiftAssign,
        //        ExpressionType.SubtractAssign,
        //        ExpressionType.SubtractAssignChecked,
        //    };

        public static bool CurrentHasPrecedence(JavascriptOperationTypes current, JavascriptOperationTypes parent)
        {
            if (current == JavascriptOperationTypes.Call && parent == JavascriptOperationTypes.IndexerProperty)
                return true;

            if (current == JavascriptOperationTypes.TernaryCondition)
                return JavascriptOperationTypes.TernaryCondition > parent;

            if (current == JavascriptOperationTypes.AddSubtract && parent == JavascriptOperationTypes.Concat)
                return false;
            if (current == JavascriptOperationTypes.Concat && parent == JavascriptOperationTypes.AddSubtract)
                return false;

            return current >= parent;
        }

        public static JavascriptOperationTypes GetJsOperator(ExpressionType nodeType, Type type)
        {
            switch (nodeType)
            {
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    if (type == typeof(string))
                        return JavascriptOperationTypes.Concat;
                    return JavascriptOperationTypes.AddSubtract;

                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.ExclusiveOr:
                    return JavascriptOperationTypes.AndXor;

                case ExpressionType.ArrayIndex:
                case ExpressionType.ArrayLength:
                case ExpressionType.MemberAccess:
                    return JavascriptOperationTypes.IndexerProperty;

                case ExpressionType.Call:
                    return JavascriptOperationTypes.Call;

                case ExpressionType.AddAssign:
                case ExpressionType.AddAssignChecked:
                case ExpressionType.Assign:
                case ExpressionType.AndAssign:
                case ExpressionType.DivideAssign:
                case ExpressionType.ExclusiveOrAssign:
                case ExpressionType.LeftShiftAssign:
                case ExpressionType.ModuloAssign:
                case ExpressionType.MultiplyAssign:
                case ExpressionType.MultiplyAssignChecked:
                case ExpressionType.OrAssign:
                case ExpressionType.PowerAssign:
                case ExpressionType.RightShiftAssign:
                case ExpressionType.SubtractAssign:
                case ExpressionType.SubtractAssignChecked:
                    return JavascriptOperationTypes.AssignRhs;

                case ExpressionType.PostDecrementAssign:
                case ExpressionType.PostIncrementAssign:
                case ExpressionType.PreDecrementAssign:
                case ExpressionType.PreIncrementAssign:


                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return JavascriptOperationTypes.MulDivMod;

                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    return JavascriptOperationTypes.Or;

                case ExpressionType.Equal:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.NotEqual:
                    return JavascriptOperationTypes.Comparison;

                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.OnesComplement:
                case ExpressionType.UnaryPlus:
                    return JavascriptOperationTypes.NegComplPlus;

                case ExpressionType.Conditional:
                    return JavascriptOperationTypes.TernaryCondition;

                case ExpressionType.LeftShift:
                case ExpressionType.RightShift:
                    return JavascriptOperationTypes.Shift;

                case ExpressionType.Lambda:
                    return JavascriptOperationTypes.InlineFunc;

                case ExpressionType.Constant:
                    return JavascriptOperationTypes.Literal;

                case ExpressionType.Decrement:
                case ExpressionType.Increment:
                case ExpressionType.IsFalse:
                case ExpressionType.IsTrue:

                case ExpressionType.Block:

                case ExpressionType.Coalesce:

                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.DebugInfo:
                case ExpressionType.Default:

                case ExpressionType.Dynamic:
                case ExpressionType.Extension:
                case ExpressionType.Goto:

                case ExpressionType.Index:
                case ExpressionType.Invoke:
                case ExpressionType.Label:
                case ExpressionType.ListInit:
                case ExpressionType.Loop:
                case ExpressionType.MemberInit:

                case ExpressionType.New:
                case ExpressionType.NewArrayBounds:
                case ExpressionType.NewArrayInit:

                case ExpressionType.Parameter:
                case ExpressionType.Power:
                case ExpressionType.Quote:
                case ExpressionType.RuntimeVariables:
                case ExpressionType.Switch:
                case ExpressionType.Throw:
                case ExpressionType.Try:
                case ExpressionType.TypeAs:
                case ExpressionType.TypeEqual:
                case ExpressionType.TypeIs:
                case ExpressionType.Unbox:
                    return JavascriptOperationTypes.NoOp;
                default:
                    throw new ArgumentOutOfRangeException(nameof(nodeType));
            }
        }

        public static void WriteOperator(StringBuilder result, ExpressionType nodeType, Type type)
        {
            switch (nodeType)
            {
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    result.Append('+');
                    break;
                case ExpressionType.AddAssign:
                case ExpressionType.AddAssignChecked:
                    result.Append("+=");
                    break;
                case ExpressionType.And:
                    result.Append("&");
                    break;
                case ExpressionType.AndAlso:
                    result.Append("&&");
                    break;
                case ExpressionType.AndAssign:
                    result.Append("&=");
                    break;
                case ExpressionType.ArrayIndex:
                    break;
                case ExpressionType.ArrayLength:
                    result.Append(".length");
                    break;
                case ExpressionType.Assign:
                    result.Append("=");
                    break;
                case ExpressionType.Block:
                    break;
                case ExpressionType.Call:
                    break;
                case ExpressionType.Coalesce:
                    break;
                case ExpressionType.Conditional:
                    break;
                case ExpressionType.Constant:
                    break;
                case ExpressionType.Convert:
                    break;
                case ExpressionType.ConvertChecked:
                    break;
                case ExpressionType.DebugInfo:
                    break;
                case ExpressionType.Decrement:
                    result.Append("--");
                    break;
                case ExpressionType.Default:
                    break;
                case ExpressionType.Divide:
                    result.Append("/");
                    break;
                case ExpressionType.DivideAssign:
                    result.Append("/=");
                    break;
                case ExpressionType.Dynamic:
                    break;
                case ExpressionType.Equal:
                    result.Append("===");
                    break;
                case ExpressionType.ExclusiveOr:
                    result.Append("^");
                    break;
                case ExpressionType.ExclusiveOrAssign:
                    result.Append("^=");
                    break;
                case ExpressionType.Extension:
                    break;
                case ExpressionType.Goto:
                    break;
                case ExpressionType.GreaterThan:
                    result.Append(">");
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    result.Append(">=");
                    break;
                case ExpressionType.Increment:
                    result.Append("++");
                    break;
                case ExpressionType.Index:
                    break;
                case ExpressionType.Invoke:
                    break;
                case ExpressionType.IsFalse:
                    break;
                case ExpressionType.IsTrue:
                    break;
                case ExpressionType.Label:
                    break;
                case ExpressionType.Lambda:
                    break;
                case ExpressionType.LeftShift:
                    result.Append("<<");
                    break;
                case ExpressionType.LeftShiftAssign:
                    result.Append("<<=");
                    break;
                case ExpressionType.LessThan:
                    result.Append("<");
                    break;
                case ExpressionType.LessThanOrEqual:
                    result.Append("<=");
                    break;
                case ExpressionType.ListInit:
                    break;
                case ExpressionType.Loop:
                    break;
                case ExpressionType.MemberAccess:
                    break;
                case ExpressionType.MemberInit:
                    break;
                case ExpressionType.Modulo:
                    result.Append("%");
                    break;
                case ExpressionType.ModuloAssign:
                    result.Append("%=");
                    break;
                case ExpressionType.Multiply:
                    result.Append("*");
                    break;
                case ExpressionType.MultiplyAssign:
                    result.Append("*=");
                    break;
                case ExpressionType.MultiplyAssignChecked:
                    break;
                case ExpressionType.MultiplyChecked:
                    break;
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    result.Append("-");
                    break;
                case ExpressionType.New:
                    result.Append("new");
                    break;
                case ExpressionType.NewArrayBounds:
                    break;
                case ExpressionType.NewArrayInit:
                    break;
                case ExpressionType.Not:
                    if (TypeHelpers.IsNumericType(type) || type.GetTypeInfo().IsEnum)
                        result.Append("~");
                    else
                        result.Append("!");

                    break;
                case ExpressionType.NotEqual:
                    result.Append("!==");
                    break;
                case ExpressionType.OnesComplement:
                    result.Append("~");
                    break;
                case ExpressionType.Or:
                    result.Append("|");
                    break;
                case ExpressionType.OrAssign:
                    result.Append("|=");
                    break;
                case ExpressionType.OrElse:
                    result.Append("||");
                    break;
                case ExpressionType.Parameter:
                    break;
                case ExpressionType.Power:
                    break;
                case ExpressionType.PowerAssign:
                    break;
                case ExpressionType.PostDecrementAssign:
                case ExpressionType.PreDecrementAssign:
                    result.Append("--");
                    break;
                case ExpressionType.PostIncrementAssign:
                case ExpressionType.PreIncrementAssign:
                    result.Append("++");
                    break;
                case ExpressionType.Quote:
                    break;
                case ExpressionType.RightShift:
                    result.Append(">>");
                    break;
                case ExpressionType.RightShiftAssign:
                    result.Append(">>=");
                    break;
                case ExpressionType.RuntimeVariables:
                    break;
                case ExpressionType.Subtract:
                    result.Append("-");
                    break;
                case ExpressionType.SubtractAssign:
                    result.Append("-=");
                    break;
                case ExpressionType.SubtractAssignChecked:
                    result.Append("--");
                    break;
                case ExpressionType.SubtractChecked:
                    result.Append("-");
                    break;
                case ExpressionType.Switch:
                    break;
                case ExpressionType.Throw:
                    break;
                case ExpressionType.Try:
                    break;
                case ExpressionType.TypeAs:
                    break;
                case ExpressionType.TypeEqual:
                    break;
                case ExpressionType.TypeIs:
                    break;
                case ExpressionType.UnaryPlus:
                    break;
                case ExpressionType.Unbox:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}