
using System.Collections.Generic;
using System.Collections;
using Jint.Native;
namespace Jint.Expressions {
    public interface IStatementVisitor {
        void Visit(Program expression);
        void Visit(AssignmentExpression expression);
        void Visit(BlockStatement expression);
        void Visit(BreakStatement expression);
        void Visit(ContinueStatement expression);
        void Visit(DoWhileStatement expression);
        void Visit(EmptyStatement expression);
        void Visit(ExpressionStatement expression);
        void Visit(ForEachInStatement expression);
        void Visit(ForStatement expression);
        void Visit(FunctionDeclarationStatement expression);
        void Visit(IfStatement expression);
        void Visit(ReturnStatement expression);
        void Visit(SwitchStatement expression);
        void Visit(WithStatement expression);
        void Visit(ThrowStatement expression);
        void Visit(TryStatement expression);
        void Visit(VariableDeclarationStatement expression);
        void Visit(WhileStatement expression);
        void Visit(ArrayDeclaration expression);
        void Visit(CommaOperatorStatement expression);

        void Visit(FunctionExpression expression);
        void Visit(MemberExpression expression);
        void Visit(MethodCall expression);
        void Visit(Indexer expression);
        void Visit(PropertyExpression expression);
        void Visit(PropertyDeclarationExpression expression);
        void Visit(Identifier expression);

        void Visit(JsonExpression expression);
        void Visit(NewExpression expression);
        void Visit(BinaryExpression expression);
        void Visit(TernaryExpression expression);
        void Visit(UnaryExpression expression);
        void Visit(ValueExpression expression);
        void Visit(RegexpExpression expression);
        void Visit(Statement expression);

    }

    public interface IJintVisitor {

        bool DebugMode { get; }

        JsInstance Result { get; set; }
        JsDictionaryObject CallTarget { get; }

        IGlobal Global { get; }

        JsInstance Returned { get; }

        JsInstance Return(JsInstance result);

        void ExecuteFunction(JsFunction function, JsDictionaryObject _this, JsInstance[] _parameters);
    }
}
