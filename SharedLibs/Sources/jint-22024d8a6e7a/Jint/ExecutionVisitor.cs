using System;
using System.Collections.Generic;
using System.Text;
using Jint.Expressions;
using System.Collections;
using Jint.Native;
using Jint.Debugger;
using System.Security;

namespace Jint {

    public class ExecutionVisitor : IStatementVisitor, IJintVisitor{
        struct ResultInfo {
            public JsDictionaryObject baseObject;
            public JsInstance result;
        }

        protected internal ITypeResolver typeResolver;

        public IGlobal Global { get; private set; }
        public JsScope GlobalScope { get; private set; }

        protected Stack<JsScope> Scopes = new Stack<JsScope>();

        protected bool exit;
        protected JsInstance returnInstance;
        protected int recursionLevel;

        public event EventHandler<DebugInformation> Step;
        public Stack<string> CallStack { get; set; }
        public Statement CurrentStatement { get; set; }

        public bool DebugMode { get; set; }
        public int MaxRecursions { get; set; }
		public int MaxSteps { get; set; }


        public JsInstance Returned { get { return returnInstance; } }
        public bool AllowClr { get; set; }
        public PermissionSet PermissionSet { get; set; }
        
        private StringBuilder typeFullname;
        private string lastIdentifier = String.Empty;

        ResultInfo lastResult;
        Stack<ResultInfo> stackResults = new Stack<ResultInfo>();

        public JsDictionaryObject CallTarget {
            get {
                return lastResult.baseObject;
            }
        }
        public JsInstance Result {
            get {
                return lastResult.result;
            }
            set {
                lastResult.result = value;
                lastResult.baseObject = null;
            }
        }

        public void SetResult(JsInstance value, JsDictionaryObject baseObject) {
            lastResult.result = value;
            lastResult.baseObject = baseObject;
        }

        public ExecutionVisitor(Options options) {
            typeResolver = CachedTypeResolver.Default;

            Global = new JsGlobal(this, options);
            GlobalScope = new JsScope(Global as JsObject);

            EnterScope(GlobalScope);

            CallStack = new Stack<string>();
        }

        public ExecutionVisitor(IGlobal GlobalObject, JsScope Scope) {
            if (GlobalObject == null)
                throw new ArgumentNullException("GlobalObject");
            if (Scope == null)
                throw new ArgumentNullException("Scope");

            typeResolver = CachedTypeResolver.Default;

            Global = GlobalObject;
            GlobalScope = Scope.Global;
            MaxRecursions = 500;

            EnterScope(Scope);
            CallStack = new Stack<string>();
        }

        public void OnStep(DebugInformation info) {
            if (Step != null && info.CurrentStatement != null && info.CurrentStatement.Source != null) {
                Step(this, info);
            }
        }

        public DebugInformation CreateDebugInformation(Statement statement) {
            DebugInformation info = new DebugInformation();
            info.CurrentStatement = statement;
            info.CallStack = CallStack;
            info.Locals = new JsObject(JsNull.Instance);
            DebugMode = false;

            foreach (var property in CurrentScope.GetKeys())
                info.Locals[property] = CurrentScope[property];

            DebugMode = true;

            return info;
        }

        public JsScope CurrentScope {
            get { return Scopes.Peek(); }
        }


	    protected void EnterScope(JsDictionaryObject scope) {
            Scopes.Push(new JsScope(CurrentScope, scope));
        }

        protected void EnterScope(JsScope scope) {
            Scopes.Push(scope);
        }

        protected void ExitScope() {
            Scopes.Pop();
        }

        public void Visit(Program program) {
            // initialize local variables, in case the visitor is used multiple times by the same engine
            typeFullname = null;
            exit = false;
            lastIdentifier = String.Empty;

			EnsureSteps();
			foreach (var statement in program.Statements)
			{
                CurrentStatement = statement;

                if (DebugMode) {
                    OnStep(CreateDebugInformation(statement));
                }
				EnsureSteps();
                Result = null;
                statement.Accept(this);

                if (exit) {
                    exit = false;
                    return;
                }
            }
        }

	    private void EnsureSteps()
	    {
		    if (steps++ > MaxSteps)
		    {
			    throw new JsException(Global.ErrorClass.New("Too many steps in script"));
		    }
	    }


	    public void Visit(AssignmentExpression statement) {
            switch (statement.AssignmentOperator) {
                case AssignmentOperator.Assign: statement.Right.Accept(this);
                    break;
                case AssignmentOperator.Multiply: new BinaryExpression(BinaryExpressionType.Times, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.Divide: new BinaryExpression(BinaryExpressionType.Div, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.Modulo: new BinaryExpression(BinaryExpressionType.Modulo, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.Add: new BinaryExpression(BinaryExpressionType.Plus, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.Substract: new BinaryExpression(BinaryExpressionType.Minus, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.ShiftLeft: new BinaryExpression(BinaryExpressionType.LeftShift, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.ShiftRight: new BinaryExpression(BinaryExpressionType.RightShift, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.UnsignedRightShift: new BinaryExpression(BinaryExpressionType.UnsignedRightShift, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.And: new BinaryExpression(BinaryExpressionType.BitwiseAnd, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.Or: new BinaryExpression(BinaryExpressionType.BitwiseOr, statement.Left, statement.Right).Accept(this);
                    break;
                case AssignmentOperator.XOr: new BinaryExpression(BinaryExpressionType.BitwiseXOr, statement.Left, statement.Right).Accept(this);
                    break;
                default: throw new NotSupportedException();
            }

            JsInstance right = Result;

            MemberExpression left = statement.Left as MemberExpression;
            if (left == null) {
                left = new MemberExpression(statement.Left, null);
            }

            Assign(left, right);

            Result = right;
        }

        public void Assign(MemberExpression left, JsInstance value) {
            string propertyName;
            Descriptor d = null;

            if (!(left.Member is IAssignable)) {
                throw new JintException("The left member of an assignment must be a member");
            }

            EnsureIdentifierIsDefined(value);

            JsDictionaryObject baseObject;

            if (left.Previous != null) {
                // if this a property
                left.Previous.Accept(this);
                baseObject = Result as JsDictionaryObject;

                if (baseObject == null)
                    throw new JintException("Attempt to assign to an undefined variable.");
            }
            else {
                baseObject = CurrentScope;
                // this a variable
                propertyName = ((Identifier)left.Member).Text;

                CurrentScope.TryGetDescriptor(propertyName, out d);

                if (d == null && HasOption(Options.Strict)) {
                    throw new JsException(Global.ReferenceErrorClass.New(propertyName + " is not defined"));
                }
            }

            // now baseObject contains an object or a scope against which to resolve left.Member

            if (left.Member is Identifier) {
                propertyName = ((Identifier)left.Member).Text;

                // Assigning function Name
                //if (value.Class == JsInstance.CLASS_FUNCTION)
                //    ((JsFunction)value).Name = propertyName;

                Result = baseObject[propertyName] = value;
            }
            else {
                Indexer indexer = left.Member as Indexer;

                // calculate index expression
                indexer.Index.Accept(this);

                if (baseObject is JsObject)
                {
                    JsObject target = baseObject as JsObject;
                    if (target.Indexer != null)
                    {
                        target.Indexer.set(target, Result, value);
                        Result = value;
                        return;
                    }
                }

                // Assigning function Name
                //if (value.Class == JsInstance.CLASS_FUNCTION)
                //    ((JsFunction)value).Name = Result.Value.ToString();
                Result = baseObject[Result] = value;
            }
        }

        public void Visit(CommaOperatorStatement statement) {
			EnsureSteps();
			foreach (var s in statement.Statements)
			{
                if (DebugMode) {
                    OnStep(CreateDebugInformation(s));
                }
				EnsureSteps();
                s.Accept(this);

                if (StopStatementFlow()) {
                    return;
                }
            }
        }

        public void Visit(BlockStatement statement) {
            Statement oldStatement = CurrentStatement;
			EnsureSteps();
			foreach (var s in statement.Statements)
			{
                CurrentStatement = s;

                if (DebugMode) {
                    OnStep(CreateDebugInformation(s));
                }
				EnsureSteps();
                Result = null;
                typeFullname = null;

                s.Accept(this);

                if (StopStatementFlow()) {
                    return;
                }
            }
            CurrentStatement = oldStatement;
        }

        protected ContinueStatement continueStatement = null;
        public void Visit(ContinueStatement statement) {
            continueStatement = statement;
        }

        protected BreakStatement breakStatement = null;
	    private int steps;

	    public void Visit(BreakStatement statement) {
            breakStatement = statement;
        }

        public void Visit(DoWhileStatement statement) {
            do {
                statement.Statement.Accept(this);

                ResetContinueIfPresent(statement.Label);

                if (StopStatementFlow()) {
                    if (breakStatement != null && statement.Label == breakStatement.Label) {
                        breakStatement = null;
                    }

                    return;
                }

                statement.Condition.Accept(this);

                EnsureIdentifierIsDefined(Result);

            } while (Result.ToBoolean());
        }

        public void Visit(EmptyStatement statement) {
            return;
        }

        [System.Diagnostics.DebuggerStepThrough]
        public void Visit(ExpressionStatement statement) {
            statement.Expression.Accept(this);
        }

        public void Visit(ForEachInStatement statement) {
            // todo: may be declare own property in the current scope if not a globalDeclaration?
            bool globalDeclaration = true;
            string identifier = String.Empty;

            if (statement.InitialisationStatement is VariableDeclarationStatement) {
                globalDeclaration = ((VariableDeclarationStatement)statement.InitialisationStatement).Global;
                identifier = ((VariableDeclarationStatement)statement.InitialisationStatement).Identifier;
            }
            else if (statement.InitialisationStatement is Identifier) {
                globalDeclaration = true;
                identifier = ((Identifier)statement.InitialisationStatement).Text;
            }
            else {
                throw new NotSupportedException("Only variable declaration are allowed in a for in loop");
            }

            statement.Expression.Accept(this);

            var dictionary = Result as JsDictionaryObject;

            if (Result.Value is IEnumerable) {
                foreach (object value in (IEnumerable)Result.Value) {
                    CurrentScope[identifier] = Global.WrapClr(value);

                    statement.Statement.Accept(this);

                    ResetContinueIfPresent(statement.Label);

                    if (StopStatementFlow()) {
                        if (breakStatement != null && statement.Label == breakStatement.Label) {
                            breakStatement = null;
                        }

                        return;
                    }

                    ResetContinueIfPresent(statement.Label);
                }
            }
            else if (dictionary != null) {
                List<string> keys = new List<string>(dictionary.GetKeys());

                // Uses a for loop as it might be changed by the inner statements
                for (int i = 0; i < keys.Count; i++) {
                    string value = keys[i];

                    CurrentScope[identifier] = Global.StringClass.New(value);

                    statement.Statement.Accept(this);

                    ResetContinueIfPresent(statement.Label);

                    if (StopStatementFlow()) {
                        if (breakStatement != null && statement.Label == breakStatement.Label) {
                            breakStatement = null;
                        }

                        return;
                    }

                    ResetContinueIfPresent(statement.Label);
                }
            }
            else {
                throw new InvalidOperationException("The property can't be enumerated");
            }

        }

        public void Visit(WithStatement statement) {
            statement.Expression.Accept(this);

            if (!(Result is JsDictionaryObject)) {
                throw new JsException(Global.StringClass.New("Invalid expression in 'with' statement"));
            }

            EnterScope((JsDictionaryObject)Result);

            try {
                statement.Statement.Accept(this);
            }
            finally {
                ExitScope();
            }
        }

        public void Visit(ForStatement statement) {
            if (statement.InitialisationStatement != null)
                statement.InitialisationStatement.Accept(this);

            if (statement.ConditionExpression != null)
                statement.ConditionExpression.Accept(this);
            else
                Result = Global.BooleanClass.New(true);

            EnsureIdentifierIsDefined(Result);

            while (Result.ToBoolean()) {
                statement.Statement.Accept(this);

                ResetContinueIfPresent(statement.Label);

                if (StopStatementFlow()) {
                    if (breakStatement != null && statement.Label == breakStatement.Label) {
                        breakStatement = null;
                    }

                    return;
                }

                // Goes back in the scopes so that the variables are accessible after the statement
                if (statement.IncrementExpression != null)
                    statement.IncrementExpression.Accept(this);

                if (statement.ConditionExpression != null)
                    statement.ConditionExpression.Accept(this);
                else
                    Result = Global.BooleanClass.New(true);

            }
        }

        public JsFunction CreateFunction(IFunctionDeclaration functionDeclaration) {
            JsFunction f = Global.FunctionClass.New();

            var statementsWithDefaultReturn = new BlockStatement();
            
            // injects a default return statement at the end of each function
            statementsWithDefaultReturn.Statements.AddLast(functionDeclaration.Statement);
            statementsWithDefaultReturn.Statements.AddLast(new ReturnStatement(new Identifier("undefined")));
            f.Statement = statementsWithDefaultReturn;

            f.Name = functionDeclaration.Name;
            f.Scope = CurrentScope; // copy current scope hierarchy

            f.Arguments = functionDeclaration.Parameters;
            if (HasOption(Options.Strict)) {
                foreach (string arg in f.Arguments) {
                    if (arg == "eval" || arg == "arguments")
                        throw new JsException(Global.StringClass.New("The parameters do not respect strict mode"));
                }
            }

            return f;
        }

        public void Visit(FunctionDeclarationStatement statement) {
            JsFunction f = CreateFunction(statement);
            CurrentScope.DefineOwnProperty(statement.Name, f);
        }

        public void Visit(IfStatement statement) {
            statement.Expression.Accept(this);
            
            EnsureIdentifierIsDefined(Result);
            
            if (Result.ToBoolean()) {
                statement.Then.Accept(this);
            }
            else {
                if (statement.Else != null) {
                    statement.Else.Accept(this);
                }
            }
        }

        public void Visit(ReturnStatement statement) {
            if (statement.Expression != null) {
                statement.Expression.Accept(this);
                Return(Result);
            }

            exit = true;
        }

        public JsInstance Return(JsInstance instance) {
            returnInstance = instance;
            return returnInstance;
        }

        public void Visit(SwitchStatement statement) {
            CurrentStatement = statement.Expression;

            bool found = false;
            if (statement.CaseClauses != null) {
                foreach (var clause in statement.CaseClauses) {
                    CurrentStatement = clause.Expression;

                    if (found) {
                        // jumping from one case to the next one
                        clause.Statements.Accept(this);
                        if (exit)
                            break;
                    } else {
                        new BinaryExpression(BinaryExpressionType.Equal, (Expression)statement.Expression, clause.Expression).Accept(this);
                        if (Result.ToBoolean()) {
                            clause.Statements.Accept(this);
                            found = true;
                            if (exit)
                                break;
                        }
                    }

                    if (breakStatement != null) {
                        breakStatement = null;
                        break;
                    }
                }
            }

            if (!found && statement.DefaultStatements!= null) {
                statement.DefaultStatements.Accept(this);

                // handle break statements in default case by clearing it
                if (breakStatement != null) {
                    breakStatement = null;
                }
            }
        }

        public void Visit(ThrowStatement statement) {
            Result = JsUndefined.Instance;

            if (statement.Expression != null) {
                statement.Expression.Accept(this);
            }

            throw new JsException(Result);
        }

        public void Visit(TryStatement statement) {
            try {
                statement.Statement.Accept(this);
            }
            catch (Exception e) {
                // there might be no catch statement defined
                if (statement.Catch != null) {
                    JsException jsException = e as JsException;

                    if (jsException == null)
                        jsException = new JsException(Global.ErrorClass.New(e.Message));

                    // handle thrown exception assignment to a local variable: catch(e)
                    if (statement.Catch.Identifier != null) {
                        // if catch is called, Result contains the thrown value
                        CurrentScope.DefineOwnProperty(statement.Catch.Identifier, JsUndefined.Instance);
                        Assign(new MemberExpression(new PropertyExpression(statement.Catch.Identifier), null), jsException.Value);
                    }

                    statement.Catch.Statement.Accept(this);
                }
                else {
                    throw;
                }
            }
            finally {

                if (statement.Finally != null) {
                    JsObject catchScope = new JsObject();
                    statement.Finally.Statement.Accept(this);
                }
            }

        }

        public void Visit(VariableDeclarationStatement statement) {
            Result = JsUndefined.Instance;

            // if the right expression is not defined, declare the variable as undefined
            if (statement.Expression != null) {
                statement.Expression.Accept(this);
                if (statement.Global) {
                    throw new InvalidOperationException("Cant declare a global variable");
                    // todo: where is it from? 
                }
                else {
                    if (!CurrentScope.HasOwnProperty(statement.Identifier))
                        CurrentScope.DefineOwnProperty(statement.Identifier, Result);
                    else
                        CurrentScope[statement.Identifier] = Result;
                }
            }
            else {
                // a var declaration should not affect existing one
                if (!CurrentScope.HasOwnProperty(statement.Identifier))
                    CurrentScope.DefineOwnProperty(statement.Identifier, JsUndefined.Instance);
            }



        }

        public void Visit(WhileStatement statement) {
            statement.Condition.Accept(this);

            EnsureIdentifierIsDefined(Result);

            while (Result.ToBoolean()) {
                statement.Statement.Accept(this);

                ResetContinueIfPresent(statement.Label);

                if (StopStatementFlow()) {
                    if (breakStatement != null && statement.Label == breakStatement.Label) {
                        breakStatement = null;
                    }

                    return;
                }

                statement.Condition.Accept(this);
            }
        }

        public void Visit(NewExpression expression) {

            Result = null;

            expression.Expression.Accept(this);

            if (AllowClr && Result == JsUndefined.Instance && typeFullname != null && typeFullname.Length > 0 && expression.Generics.Count > 0)
            {
                string typeName = typeFullname.ToString();
                typeFullname = new StringBuilder();

                var genericParameters = new Type[expression.Generics.Count];

                try
                {
                    int i = 0;
                    foreach (Expression generic in expression.Generics)
                    {
                        generic.Accept(this);
                        genericParameters[i] = Global.Marshaller.MarshalJsValue<Type>(Result);
                        i++;
                    }
                }
                catch (Exception e)
                {
                    throw new JintException("A type parameter is required", e);
                }

                typeName += "`" + genericParameters.Length;
                Result = Global.Marshaller.MarshalClrValue<Type>(typeResolver.ResolveType(typeName).MakeGenericType(genericParameters));
            }

            if (Result != null && Result is JsFunction) {
                JsFunction function = (JsFunction)Result;
                
                // Process parameters
                JsInstance[] parameters = new JsInstance[expression.Arguments.Count];

                for (int i = 0; i < expression.Arguments.Count; i++) {
                    expression.Arguments[i].Accept(this);
                    parameters[i] = Result;
                }

                Result = function.Construct(parameters, null, this);

                return;
            } else
                throw new JsException(Global.ErrorClass.New("Function expected."));
        }

        public void Visit(TernaryExpression expression) {
            Result = null;

            // Evaluates the left expression and saves the value
            expression.LeftExpression.Accept(this);
            var left = Result;

            Result = null;

            EnsureIdentifierIsDefined(left);

            if (left.ToBoolean()) {
                // Evaluates the middle expression
                expression.MiddleExpression.Accept(this);
            }
            else {
                // Evaluates the right expression
                expression.RightExpression.Accept(this);
            }
        }

        public static bool IsNullOrUndefined(JsInstance o) {
            return (o == JsUndefined.Instance) || (o == JsNull.Instance) || (o.IsClr && o.Value == null);
        }

        public JsBoolean Compare(JsInstance x, JsInstance y)
        {
            if (x.IsClr && y.IsClr)
            {
                return Global.BooleanClass.New(x.Value.Equals(y.Value));
            }

            // if one of the arguments is a native js object, we should
            // apply an ecma compare rules
            /* if (x.IsClr)
            {
                return Compare(x.ToPrimitive(Global), y);
            }

            if (y.IsClr)
            {
                return Compare(x, y.ToPrimitive(Global));
            } */

            if (x.Type == y.Type)
            { // if both are Objects but then only one is Clrs
                if (x == JsUndefined.Instance)
                {
                    return Global.BooleanClass.True;
                }
                else if (x == JsNull.Instance)
                {
                    return Global.BooleanClass.True;
                }
                else if (x.Type == JsInstance.TYPE_NUMBER)
                {
                    if (x.ToNumber() == double.NaN)
                    {
                        return Global.BooleanClass.False;
                    }
                    else if (y.ToNumber() == double.NaN)
                    {
                        return Global.BooleanClass.False;
                    }
                    else if (x.ToNumber() == y.ToNumber())
                    {
                        return Global.BooleanClass.True;
                    }
                    else
                    {
                        return Global.BooleanClass.False;
                    }
                }
                else if (x.Type == JsInstance.TYPE_STRING)
                {
                    return Global.BooleanClass.New(x.ToString() == y.ToString());
                }
                else if (x.Type == JsInstance.TYPE_BOOLEAN)
                {
                    return Global.BooleanClass.New(x.ToBoolean() == y.ToBoolean());
                }
                else if (x.Type == JsInstance.TYPE_OBJECT )
                {
                    return Global.BooleanClass.New(x == y);
                }
                else
                {
                    return Global.BooleanClass.New(x.Value.Equals(y.Value));
                }
            }
            else if (x == JsNull.Instance && y == JsUndefined.Instance)
            {
                return Global.BooleanClass.True;
            }
            else if (x == JsUndefined.Instance && y == JsNull.Instance)
            {
                return Global.BooleanClass.True;
            }
            else if (x.Type == JsInstance.TYPE_NUMBER && y.Type == JsInstance.TYPE_STRING)
            {
                return Global.BooleanClass.New(x.ToNumber() == y.ToNumber());
            }
            else if (x.Type == JsInstance.TYPE_STRING && y.Type == JsInstance.TYPE_NUMBER)
            {
                return Global.BooleanClass.New(x.ToNumber() == y.ToNumber());
            }
            else if (x.Type == JsInstance.TYPE_BOOLEAN || y.Type == JsInstance.TYPE_BOOLEAN)
            {
                return Global.BooleanClass.New(x.ToNumber() == y.ToNumber());
            }
            else if (y.Type == JsInstance.TYPE_OBJECT && (x.Type == JsInstance.TYPE_STRING || x.Type == JsInstance.TYPE_NUMBER))
            {
                return Compare(x, y.ToPrimitive(Global));
            }
            else if (x.Type == JsInstance.TYPE_OBJECT && (y.Type == JsInstance.TYPE_STRING || y.Type == JsInstance.TYPE_NUMBER))
            {
                return Compare(x.ToPrimitive(Global), y);
            }
            else
            {
                return Global.BooleanClass.False;
            }
        }

        public bool CompareTo(JsInstance x, JsInstance y, out int result) {
            result = 0;

            if (x.IsClr && y.IsClr) {
                IComparable xcmp = x.Value as IComparable;
                
                if (xcmp == null || y.Value == null || xcmp.GetType() != y.Value.GetType())
                    return false;
                result = xcmp.CompareTo(y.Value);
            } else {

                Double xnum = x.ToNumber();
                Double ynum = y.ToNumber();

                if (Double.IsNaN(xnum) || Double.IsNaN(ynum))
                    return false;

                if (xnum < ynum)
                    result = -1;
                else if (xnum == ynum)
                    result = 0;
                else
                    result = 1;
            }
            return true;
        }
        

        public void Visit(BinaryExpression expression) {
            // Evaluates the left expression and saves the value
            expression.LeftExpression.Accept(this);

            EnsureIdentifierIsDefined(Result);

            JsInstance left = Result;

            //prevents execution of the right hand side if false
            if (expression.Type == BinaryExpressionType.And && !left.ToBoolean()) {
                Result = left;
                return;
            }

            //prevents execution of the right hand side if true
            if (expression.Type == BinaryExpressionType.Or && left.ToBoolean()) {
                Result = left;
                return;
            }

            // Evaluates the right expression and saves the value
            expression.RightExpression.Accept(this);

            EnsureIdentifierIsDefined(Result);

            JsInstance right = Result;
            int cmpResult;

            switch (expression.Type) {
                case BinaryExpressionType.And:

                    if (left.ToBoolean()) {
                        Result = right;
                    }
                    else {
                        Result = Global.BooleanClass.False;
                    }

                    break;

                case BinaryExpressionType.Or:
                    if (left.ToBoolean()) {
                        Result = left;
                    }
                    else {
                        Result = right;
                    }

                    break;

                case BinaryExpressionType.Div:
                    var rightNumber = right.ToNumber();
                    var leftNumber = left.ToNumber();

                    if (right == Global.NumberClass["NEGATIVE_INFINITY"] || right == Global.NumberClass["POSITIVE_INFINITY"]) {
                        Result = Global.NumberClass.New(0);
                    }
                    else if (rightNumber == 0) {
                        Result = leftNumber > 0 ? Global.NumberClass["POSITIVE_INFINITY"] : Global.NumberClass["NEGATIVE_INFINITY"];
                    }
                    else {
                        Result = Global.NumberClass.New(leftNumber / rightNumber);
                    }
                    break;

                case BinaryExpressionType.Equal:
                    Result = Compare(left, right);

                    break;

                case BinaryExpressionType.Greater:
                    Result = CompareTo(left,right,out cmpResult) && cmpResult > 0 ? Global.BooleanClass.True : Global.BooleanClass.False ;
                    break;

                case BinaryExpressionType.GreaterOrEqual:
                    Result = CompareTo(left, right, out cmpResult) && cmpResult >= 0 ? Global.BooleanClass.True : Global.BooleanClass.False;
                    break;

                case BinaryExpressionType.Lesser:
                    Result = CompareTo(left, right, out cmpResult) && cmpResult < 0 ? Global.BooleanClass.True : Global.BooleanClass.False;
                    break;

                case BinaryExpressionType.LesserOrEqual:
                    Result = CompareTo(left, right, out cmpResult) && cmpResult <= 0 ? Global.BooleanClass.True : Global.BooleanClass.False;
                    break;

                case BinaryExpressionType.Minus:
                    Result = Global.NumberClass.New(left.ToNumber() - right.ToNumber());
                    break;

                case BinaryExpressionType.Modulo:
                    if (right == Global.NumberClass["NEGATIVE_INFINITY"] || right == Global.NumberClass["POSITIVE_INFINITY"]) {
                        Result = Global.NumberClass["POSITIVE_INFINITY"];
                    }
                    else if (right.ToNumber() == 0) {
                        Result = Global.NumberClass["NaN"];
                    }
                    else {
                        Result = Global.NumberClass.New(left.ToNumber() % right.ToNumber());
                    }
                    break;

                case BinaryExpressionType.NotEqual:

                    Result = Global.BooleanClass.New(!Compare(left, right).ToBoolean());

                    break;

                case BinaryExpressionType.Plus:
                    {
                        JsInstance lprim = left.ToPrimitive(Global);
                        JsInstance rprim = right.ToPrimitive(Global);

                        if (lprim.Class == JsInstance.CLASS_STRING || rprim.Class == JsInstance.CLASS_STRING)
                            Result = Global.StringClass.New(String.Concat(lprim.ToString(), rprim.ToString()));
                        else
                            Result = Global.NumberClass.New(lprim.ToNumber() + rprim.ToNumber());
                    }
                    break;
                
                case BinaryExpressionType.Times:
                    Result = Global.NumberClass.New(left.ToNumber() * right.ToNumber());
                    break;

                case BinaryExpressionType.Pow:
                    Result = Global.NumberClass.New(Math.Pow(left.ToNumber(), right.ToNumber()));
                    break;

                case BinaryExpressionType.BitwiseAnd:
                    if (left == JsUndefined.Instance || right == JsUndefined.Instance)
                        Result = Global.NumberClass.New(0);
                    else
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()) & Convert.ToInt64(right.ToNumber()));
                    break;

                case BinaryExpressionType.BitwiseOr:
                    if (left == JsUndefined.Instance) {
                        if(right == JsUndefined.Instance)
                            Result = Global.NumberClass.New(1);
                        else
                            Result = Global.NumberClass.New(Convert.ToInt64(right.ToNumber()));
                    }
                    else if (right == JsUndefined.Instance)
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()));
                    else
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()) | Convert.ToInt64(right.ToNumber()));
                    break;

                case BinaryExpressionType.BitwiseXOr:
                    if (left == JsUndefined.Instance) {
                        if(right == JsUndefined.Instance)
                            Result = Global.NumberClass.New(1);
                        else
                            Result = Global.NumberClass.New(Convert.ToInt64(right.ToNumber()));
                    }
                    else if (right == JsUndefined.Instance)
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()));
                    else
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()) ^ Convert.ToInt64(right.ToNumber()));
                    break;

                case BinaryExpressionType.Same:
                    Result = JsInstance.StrictlyEquals(Global, left, right);

                    break;

                case BinaryExpressionType.NotSame:
                    new BinaryExpression(BinaryExpressionType.Same, expression.LeftExpression, expression.RightExpression).Accept(this);
                    Result = Global.BooleanClass.New(!Result.ToBoolean());
                    break;

                case BinaryExpressionType.LeftShift:
                    if (left == JsUndefined.Instance)
                        Result = Global.NumberClass.New(0);
                    else if (right == JsUndefined.Instance)
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()));
                    else
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()) << Convert.ToUInt16(right.ToNumber()));
                    break;

                case BinaryExpressionType.RightShift:
                    if (left == JsUndefined.Instance)
                        Result = Global.NumberClass.New(0);
                    else if (right == JsUndefined.Instance)
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()));
                    else
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()) >> Convert.ToUInt16(right.ToNumber()));
                    break;

                case BinaryExpressionType.UnsignedRightShift:
                    if (left == JsUndefined.Instance)
                        Result = Global.NumberClass.New(0);
                    else if (right == JsUndefined.Instance)
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()));
                    else
                        Result = Global.NumberClass.New(Convert.ToInt64(left.ToNumber()) >> Convert.ToUInt16(right.ToNumber()));
                    break;

                case BinaryExpressionType.InstanceOf: {
                        var func = right as JsFunction;
                        var obj = left as JsObject;
                        if (func == null)
                            throw new JsException(Global.TypeErrorClass.New("Right argument should be a function: " + expression.RightExpression.ToString()));
                        if (obj == null)
                            throw new JsException(Global.TypeErrorClass.New("Left argument should be an object: " + expression.LeftExpression.ToString()));

                        Result = Global.BooleanClass.New(func.HasInstance(obj));
                    }
                    break;

                case BinaryExpressionType.In:
                    if (right is ILiteral) {
                        throw new JsException(Global.ErrorClass.New("Cannot apply 'in' operator to the specified member."));
                    }
                    else {
                        Result = Global.BooleanClass.New(((JsDictionaryObject)right).HasProperty(left));
                    }

                    break;

                default:
                    throw new NotSupportedException("Unkown binary operator");
            }
        }

        public void Visit(UnaryExpression expression) {
            MemberExpression member;

            switch (expression.Type) {
                case UnaryExpressionType.TypeOf:

                    expression.Expression.Accept(this);

                    if (Result == null)
                        Result = Global.StringClass.New(JsUndefined.Instance.Type);
                    else if (Result is JsNull)
                        Result = Global.StringClass.New(JsInstance.TYPE_OBJECT);
                    else if (Result is JsFunction)
                        Result = Global.StringClass.New(JsInstance.TYPEOF_FUNCTION);
                    else
                        Result = Global.StringClass.New(Result.Type);

                    break;

                case UnaryExpressionType.Not:
                    expression.Expression.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    Result = Global.BooleanClass.New(!Result.ToBoolean());
                    break;

                case UnaryExpressionType.Negate:
                    expression.Expression.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    Result = Global.NumberClass.New(-Result.ToNumber());
                    break;

                case UnaryExpressionType.Positive:
                    expression.Expression.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    Result = Global.NumberClass.New(+Result.ToNumber());
                    break;

                case UnaryExpressionType.PostfixPlusPlus:

                    expression.Expression.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    JsInstance value = Result;

                    member = expression.Expression as MemberExpression ?? new MemberExpression(expression.Expression, null);

                    Assign(member, Global.NumberClass.New(value.ToNumber() + 1));

                    Result = value;

                    break;

                case UnaryExpressionType.PostfixMinusMinus:

                    expression.Expression.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    value = Result;

                    member = expression.Expression as MemberExpression ?? new MemberExpression(expression.Expression, null);

                    Assign(member, Global.NumberClass.New(value.ToNumber() - 1));

                    Result = value;

                    break;

                case UnaryExpressionType.PrefixPlusPlus:

                    expression.Expression.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    value = Global.NumberClass.New(Result.ToNumber() + 1);

                    member = expression.Expression as MemberExpression ?? new MemberExpression(expression.Expression, null);
                    Assign(member, value);

                    break;

                case UnaryExpressionType.PrefixMinusMinus:

                    expression.Expression.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    value = Global.NumberClass.New(Result.ToNumber() - 1);

                    member = expression.Expression as MemberExpression ?? new MemberExpression(expression.Expression, null);
                    Assign(member, value);

                    break;

                case UnaryExpressionType.Delete:

                    member = expression.Expression as MemberExpression;
                    if (member == null)
                        throw new NotImplementedException("delete");
                    member.Previous.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    value = Result;
                    string propertyName = null;
                    if (member.Member is PropertyExpression)
                        propertyName = ((PropertyExpression)member.Member).Text;
                    if (member.Member is Indexer) {
                        ((Indexer)member.Member).Index.Accept(this);
                        propertyName = Result.ToString();
                    }
                    if (string.IsNullOrEmpty(propertyName))
                        throw new JsException(Global.TypeErrorClass.New());
                    try {
                        ((JsDictionaryObject)value).Delete(propertyName);
                    }
                    catch (JintException) {
                        throw new JsException(Global.TypeErrorClass.New());
                    }
                    Result = value;
                    break;

                case UnaryExpressionType.Void:

                    expression.Expression.Accept(this);
                    Result = JsUndefined.Instance;
                    break;

                case UnaryExpressionType.Inv:

                    expression.Expression.Accept(this);
                    EnsureIdentifierIsDefined(Result);
                    Result = Global.NumberClass.New(0 - Result.ToNumber() - 1);
                    break;

            }
        }

        public void Visit(ValueExpression expression) {
            switch (expression.TypeCode) {
                case TypeCode.Boolean: Result = Global.BooleanClass.New((bool)expression.Value); break;
                case TypeCode.Int32:
                case TypeCode.Single:
                case TypeCode.Double: Result = Global.NumberClass.New(Convert.ToDouble(expression.Value)); break;
                case TypeCode.String: Result = Global.StringClass.New((string)expression.Value); break;
                default: Result = expression.Value as JsInstance;
                    break;
            }
        }

        public void Visit(FunctionExpression fe) {
            Result = CreateFunction(fe);
        }

        public void Visit(Statement expression) {
            // fallback for an unsupported expression
            throw new NotImplementedException();
        }

        public void Visit(MemberExpression expression) {
	        if (expression.Previous != null) {
                // the previous part is an property, it will set a callTarget
                expression.Previous.Accept(this);
            }

            expression.Member.Accept(this);

            // Try to evaluate a CLR type
            if (AllowClr && Result == JsUndefined.Instance && typeFullname != null && typeFullname.Length > 0) {
                EnsureClrAllowed();

                Type type = typeResolver.ResolveType(typeFullname.ToString());

                if (type != null) {
                    Result = Global.WrapClr(type);
                    typeFullname = new StringBuilder();
                }
            }
        }

        public void EnsureIdentifierIsDefined(object value) {
            if (value == null) {
                throw new JsException(Global.ReferenceErrorClass.New(lastIdentifier + " is not defined"));
            }
        }

        public void Visit(Indexer indexer) {
            EnsureIdentifierIsDefined(Result);

            JsObject target = (JsObject)Result;

            indexer.Index.Accept(this);

            if (target.IsClr)
                EnsureClrAllowed();

            if (target.Class == JsInstance.CLASS_STRING)
            {
                try
                {
                    SetResult(Global.StringClass.New(target.ToString()[Convert.ToInt32(Result.ToNumber())].ToString()), target);
                    return;
                }
                catch
                {
                    // if an error occured, try to access the index as a member
                }
            }

            if (target.Indexer != null)
                SetResult(target.Indexer.get(target, Result), target);
            else
                SetResult(target[Result], target);
        }

        public void Visit(MethodCall methodCall) {
            var that = CallTarget;
            var target = Result;

            if (target == JsUndefined.Instance || Result == null) {
                if (String.IsNullOrEmpty(lastIdentifier)) {
                    throw new JsException(Global.TypeErrorClass.New("Method isn't defined"));
                }
            }

            Type[] genericParameters = null;

            if (AllowClr && methodCall.Generics.Count > 0)
            {
                genericParameters = new Type[methodCall.Generics.Count];

                try
                {
                    var i = 0;
                    foreach (var generic in methodCall.Generics)
                    {
                        generic.Accept(this);
                        genericParameters[i] = Global.Marshaller.MarshalJsValue<Type>(Result);
                        i++;
                    }
                }
                catch (Exception e)
                {
                    throw new JintException("A type parameter is required", e);
                }
            }

            #region Evaluates parameters
            var parameters = new JsInstance[methodCall.Arguments.Count];

            if (methodCall.Arguments.Count > 0) {

                for (int j = 0; j < methodCall.Arguments.Count; j++) {
                    methodCall.Arguments[j].Accept(this);
                    parameters[j] = Result;
                }

            }
            #endregion

            var function = target as JsFunction;
            if (function != null)
            {
                #region DebugMode
                if (DebugMode) {
                    var stack = function.Name + "(";
                    var paramStrings = new string[parameters.Length];

                    for (int i = 0; i < parameters.Length; i++) {
                        if (parameters[i] != null)
                            paramStrings[i] = parameters[i].ToSource();
                        else
                            paramStrings[i] = "null";
                    }

                    stack += String.Join(", ", paramStrings);
                    stack += ")";
                    CallStack.Push(stack);
                }
                #endregion

                returnInstance = JsUndefined.Instance;

                var original = new JsInstance[parameters.Length];
                parameters.CopyTo(original, 0);

                ExecuteFunction(function, that, parameters, genericParameters);

                for (var i = 0; i < original.Length; i++)
                    if (original[i] != parameters[i]) {
                        if (methodCall.Arguments[i] is MemberExpression && ((MemberExpression)methodCall.Arguments[i]).Member is IAssignable)
                        {
                            Assign((MemberExpression) methodCall.Arguments[i], parameters[i]);
                        }
                        else if (methodCall.Arguments[i] is Identifier)
                        {
                            Assign(new MemberExpression(methodCall.Arguments[i], null), parameters[i]);
                        }
                    }

                #region DebugMode
                if (DebugMode) {
                    CallStack.Pop();
                }
                #endregion

                Result = returnInstance;
                returnInstance = JsUndefined.Instance;
                return;
            }
            else {
                throw new JsException(Global.ErrorClass.New("Function expected: " + lastIdentifier));
            }

        }

        public void ExecuteFunction(JsFunction function, JsDictionaryObject that, JsInstance[] parameters)
        {
            ExecuteFunction(function, that, parameters, null);
        }

        public void ExecuteFunction(JsFunction function, JsDictionaryObject that, JsInstance[] parameters, Type[] genericParameters) {
            if (function == null) {
                return;
            }

            if (recursionLevel++ > MaxRecursions) {
                throw new JsException(Global.ErrorClass.New("Too many recursions in the script."));
            }

            // ecma chapter 10.
            // TODO: move creation of the activation object to the JsFunction
            // create new argument object and instantinate arguments into it
            JsArguments args = new JsArguments(Global, function, parameters);

            // create new activation object and copy instantinated arguments to it
            // Activation should be before the function.Scope hierarchy
            JsScope functionScope = new JsScope(function.Scope ?? GlobalScope);

            for (int i = 0; i < function.Arguments.Count; i++)
                if (i < parameters.Length) 
                    functionScope.DefineOwnProperty(
                        new LinkedDescriptor(
                            functionScope,
                            function.Arguments[i],
                            args.GetDescriptor(i.ToString()),
                            args
                        )
                    );
                else
                    functionScope.DefineOwnProperty(
                        new ValueDescriptor(
                            functionScope,
                            function.Arguments[i],
                            JsUndefined.Instance
                        )
                    );

            // define arguments variable
            if (HasOption(Options.Strict))
                functionScope.DefineOwnProperty(JsScope.ARGUMENTS, args);
            else
                args.DefineOwnProperty(JsScope.ARGUMENTS, args);

            // set this variable
            if (that != null) 
                functionScope.DefineOwnProperty(JsScope.THIS, that);
            else
                functionScope.DefineOwnProperty(JsScope.THIS, that = Global as JsObject);

            // enter activation object
            EnterScope(functionScope);
            
            try {
                if (AllowClr)
                {
                    PermissionSet.PermitOnly();
                }

                if (AllowClr && genericParameters != null && genericParameters.Length > 0)
                {
                    Result = function.Execute(this, that, parameters, genericParameters);
                }
                else
                {
                    Result = function.Execute(this, that, parameters);
                }

                // Resets the return flag
                if (exit)
                {
                    exit = false;
                }
            }
            finally {
                // return to previous execution state
                ExitScope();

                if (AllowClr)
                {
                    CodeAccessPermission.RevertPermitOnly();
                }
                recursionLevel--;
            }
        }

        private bool HasOption(Options options) {
            return Global.HasOption(options);
        }


        public void Visit(PropertyExpression expression) {
            // save base of current expression
            var callTarget = Result as JsDictionaryObject;

            // this check is disabled becouse it prevents Clr names to resolve
            //if ((callTarget) == null || callTarget == JsUndefined.Instance || callTarget == JsNull.Instance)
            //{
            //    throw new JsException( Global.TypeErrorClass.New( String.Format("An object is required: {0} while resolving property {1}", lastIdentifier, expression.Text) ) );
            //}

            Result = null;

            string propertyName = lastIdentifier = expression.Text;

            JsInstance result = null;

            if (callTarget != null && callTarget.TryGetProperty(propertyName, out result)) {
                SetResult(result, callTarget);
                return;
            }

            if (Result == null && typeFullname != null && typeFullname.Length > 0) {
                typeFullname.Append('.').Append(propertyName);
            }

            SetResult(JsUndefined.Instance, callTarget);
        }

        public void Visit(PropertyDeclarationExpression expression) {
            // previous result was the object in which we need to define a property
            var target = Result as JsDictionaryObject;

            switch (expression.Mode) {
                case PropertyExpressionType.Data:
                    expression.Expression.Accept(this);
                    target.DefineOwnProperty(new ValueDescriptor(target, expression.Name, Result) );
                    break;
                case PropertyExpressionType.Get:
                case PropertyExpressionType.Set:
                    JsFunction get = null, set = null;
                    if (expression.GetExpression != null) {
                        expression.GetExpression.Accept(this);
                        get = (JsFunction)Result;
                    }
                    if (expression.SetExpression != null) {
                        expression.SetExpression.Accept(this);
                        set = (JsFunction)Result;
                    }
                    target.DefineOwnProperty(new PropertyDescriptor(Global, target, expression.Name) { GetFunction = get, SetFunction = set, Enumerable = true });
                    break;
                default:
                    break;
            }
        }

        public void Visit(Identifier expression) {
            Result = null;

            string propertyName = lastIdentifier = expression.Text;

            Descriptor result = null;
            if (CurrentScope.TryGetDescriptor(propertyName, out result)) {
                if (!result.isReference)
                    Result = result.Get(CurrentScope);
                else {
                    LinkedDescriptor r = result as LinkedDescriptor;
                    SetResult(r.Get(CurrentScope), r.targetObject);
                }

                if (Result != null)
                    return;
            }

            if (propertyName == "null") {
                Result = JsNull.Instance;
            }

            if (propertyName == "undefined") {
                Result = JsUndefined.Instance;
            }

            // Try to record full path in case it's a type
            if (Result == null) {
                if(typeFullname == null)
                {
                    typeFullname = new StringBuilder();
                }

                typeFullname.Append(propertyName);
            }
        }

        private void EnsureClrAllowed() {
            if (!AllowClr) {
                throw new SecurityException("Use of Clr is not allowed");
            }
        }

        public void Visit(JsonExpression json) {
            JsObject instance = Global.ObjectClass.New();

            foreach (var item in json.Values) {
                Result = instance;
                item.Value.Accept(this);
            }

            Result = instance;
        }

        /// <summary>
        /// Called by a loop to stop the "continue" keyword escalation
        /// </summary>
        protected void ResetContinueIfPresent(string label) {
            if (continueStatement != null && continueStatement.Label == label) {
                continueStatement = null;
            }
        }

        protected bool StopStatementFlow() {
            return exit ||
            breakStatement != null ||
            continueStatement != null;
        }

        public void Visit(ArrayDeclaration expression) {
            var array = Global.ArrayClass.New();

            // Process parameters
            JsInstance[] parameters = new JsInstance[expression.Parameters.Count];

            for (int i = 0; i < expression.Parameters.Count; i++) {
                expression.Parameters[i].Accept(this);
                array[i.ToString()] = Result;
            }

            Result = array;
        }

        public void Visit(RegexpExpression expression) {
            Result = Global.RegExpClass.New(expression.Regexp, expression.Options.Contains("g"), expression.Options.Contains("i"), expression.Options.Contains("m"));
        }


        #region IDeserializationCallback Members

        public void OnDeserialization(object sender) {
            /*
            this.methodInvoker = new CachedMethodInvoker(this);
            this.propertyGetter = new CachedReflectionPropertyGetter(methodInvoker);
            this.constructorInvoker = new CachedConstructorInvoker(methodInvoker);
            
            this.fieldGetter = new CachedReflectionFieldGetter(methodInvoker);
            */
            this.typeResolver = new CachedTypeResolver();
        }

        #endregion

	    public void ResetSteps()
	    {
		    steps = 0;
	    }
    }
}
