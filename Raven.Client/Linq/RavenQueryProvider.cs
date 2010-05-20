using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Raven.Client.Linq
{
    public class RavenQueryProvider : QueryProvider
    {
        public RavenQueryProvider()
        {
            QueryText = new StringBuilder();
            FieldsToFetch = new List<string>();
        }

        public StringBuilder QueryText { get; set; }
        public List<string> FieldsToFetch { get; set; }

        public override object Execute(Expression expression)
        {
            ProcessExpression(expression);
            Console.WriteLine(QueryText.ToString());
            return null;
        }

        public void ProcessExpression(Expression expression)
        {
            VisitExpression(expression);
        }

        private void VisitExpression(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.OrElse:
                    VisitOrElse((BinaryExpression)expression);
                    break;
                case ExpressionType.AndAlso:
                    VisitAndAlso((BinaryExpression)expression);
                    break;
                case ExpressionType.Equal:
                    VisitEqual((BinaryExpression)expression);
                    break;
                case ExpressionType.GreaterThan:
                    VisitGreaterThan((BinaryExpression)expression);
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    VisitGreaterThanOrEqual((BinaryExpression)expression);
                    break;
                case ExpressionType.LessThan:
                    VisitLessThan((BinaryExpression)expression);
                    break;
                case ExpressionType.LessThanOrEqual:
                    VisitLessThanOrEqual((BinaryExpression)expression);
                    break;
                default:
                    if (expression is MethodCallExpression)
                    {
                        VisitMethodCall((MethodCallExpression)expression);
                    }
                    else if (expression is LambdaExpression)
                    {
                        VisitExpression(((LambdaExpression)expression).Body);
                    }
                    break;
            }
        }

        private void VisitAndAlso(BinaryExpression andAlso)
        {
            VisitExpression(andAlso.Left);

            QueryText.Append("AND ");

            VisitExpression(andAlso.Right);
        }

        private void VisitOrElse(BinaryExpression orElse)
        {
            VisitExpression(orElse.Left);

            QueryText.Append("OR ");

            VisitExpression(orElse.Right);
        }

        private void VisitEqual(BinaryExpression expression)
        {
            QueryText.Append(((MemberExpression)expression.Left).Member.Name).Append(":");
            if (expression.Right.NodeType == ExpressionType.Constant)
                QueryText.Append(((ConstantExpression)expression.Right).Value);
            else if (expression.Right.NodeType == ExpressionType.MemberAccess)
                QueryText.Append(GetMemberValue((MemberExpression)expression.Right));
            else
                throw new NotSupportedException("Expression type not supported: " + expression.Right.NodeType);

            QueryText.Append(" ");
        }

        private void VisitLessThanOrEqual(BinaryExpression expression)
        {
            QueryText.Append(((MemberExpression)expression.Left).Member.Name).Append(":{* TO ");
            if (expression.Right.NodeType == ExpressionType.Constant)
                QueryText.Append(((ConstantExpression)expression.Right).Value);
            else if (expression.Right.NodeType == ExpressionType.MemberAccess)
                QueryText.Append(GetMemberValue((MemberExpression)expression.Right));
            else
                throw new NotSupportedException("Expression type not supported: " +
                                                expression.Right.NodeType);

            QueryText.Append("} ");
        }

        private void VisitLessThan(BinaryExpression expression)
        {
            QueryText.Append(((MemberExpression)expression.Left).Member.Name).Append(":[* TO ");
            if (expression.Right.NodeType == ExpressionType.Constant)
                QueryText.Append(((ConstantExpression)expression.Right).Value);
            else if (expression.Right.NodeType == ExpressionType.MemberAccess)
                QueryText.Append(GetMemberValue((MemberExpression)expression.Right));
            else
                throw new NotSupportedException("Expression type not supported: " +
                                                expression.Right.NodeType);

            QueryText.Append("] ");
        }

        private void VisitGreaterThanOrEqual(BinaryExpression expression)
        {
            QueryText.Append(((MemberExpression)expression.Left).Member.Name).Append(":{");
            if (expression.Right.NodeType == ExpressionType.Constant)
                QueryText.Append(((ConstantExpression)expression.Right).Value);
            else if (expression.Right.NodeType == ExpressionType.MemberAccess)
                QueryText.Append(GetMemberValue((MemberExpression)expression.Right));
            else
                throw new NotSupportedException("Expression type not supported: " +
                                                expression.Right.NodeType);

            QueryText.Append(" TO *} ");
        }

        private void VisitGreaterThan(BinaryExpression expression)
        {
            QueryText.Append(((MemberExpression)expression.Left).Member.Name).Append(":[");
            if (expression.Right.NodeType == ExpressionType.Constant)
                QueryText.Append(((ConstantExpression)expression.Right).Value);
            else if (expression.Right.NodeType == ExpressionType.MemberAccess)
                QueryText.Append(GetMemberValue((MemberExpression)expression.Right));
            else
                throw new NotSupportedException("Expression type not supported: " +
                                                expression.Right.NodeType);

            QueryText.Append(" TO *] ");
        }


        private void VisitMethodCall(MethodCallExpression expression)
        {
            var operand = ((UnaryExpression)expression.Arguments[1]).Operand;
            if ((expression.Method.DeclaringType == typeof(Queryable)) &&
                (expression.Method.Name == "Where"))
            {
                VisitExpression(operand);
            }
            else if ((expression.Method.DeclaringType == typeof(Queryable)) &&
                (expression.Method.Name == "Select"))
            {
                VisitExpression(expression.Arguments[0]);
                VisitSelect(operand);
            }
            else
            {
                throw new NotSupportedException("Method not supported: " + expression.Method.Name);
            }
        }

        private void VisitSelect(Expression operand)
        {
            var body = ((LambdaExpression)operand).Body;
            switch (body.NodeType)
            {
                case ExpressionType.MemberAccess:
                    FieldsToFetch.Add(((MemberExpression)body).Member.Name);
                    break;
                case ExpressionType.New:
                    FieldsToFetch.AddRange(((NewExpression)body).Arguments.Cast<MemberExpression>().Select(x => x.Member.Name));
                    break;
                default:
                    throw new NotSupportedException("Node not supported: " + body.NodeType);

            }
        }

        #region Helpers

        private static Object GetMemberValue(MemberExpression memberExpression)
        {
            MemberInfo memberInfo;
            Object obj;

            if (memberExpression == null)
                throw new ArgumentNullException("memberExpression");

            // Get object
            if (memberExpression.Expression is ConstantExpression)
                obj = ((ConstantExpression)memberExpression.Expression).Value;
            else if (memberExpression.Expression is MemberExpression)
                obj = GetMemberValue((MemberExpression)memberExpression.Expression);
            else
                throw new NotSupportedException("Expression type not supported: " +
                                                memberExpression.Expression.GetType().FullName);

            // Get value
            memberInfo = memberExpression.Member;
            if (memberInfo is PropertyInfo)
            {
                var property = (PropertyInfo)memberInfo;
                return property.GetValue(obj, null);
            }
            else if (memberInfo is FieldInfo)
            {
                var field = (FieldInfo)memberInfo;
                return field.GetValue(obj);
            }
            else
            {
                throw new NotSupportedException("MemberInfo type not supported: " + memberInfo.GetType().FullName);
            }
        }

        #endregion Helpers
    }
}