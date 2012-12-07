using System;
using System.Collections.Generic;
using System.Text;
using Jint.Expressions;
using Antlr.Runtime;
using Jint.Native;
using Jint.Debugger;
using System.Security;
using System.Security.Permissions;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Jint {
    [Serializable]
    public class JintEngine {
        protected ExecutionVisitor Visitor;

        [System.Diagnostics.DebuggerStepThrough]
        public JintEngine()
            : this(Options.Ecmascript5 | Options.Strict) {
        }

        [System.Diagnostics.DebuggerStepThrough]
        public JintEngine(Options options) {
            Visitor = new ExecutionVisitor(options);
            permissionSet = new PermissionSet(PermissionState.None);
            Visitor.AllowClr = allowClr;
            MaxRecursions = 400;
	        MaxSteps = 100 * 1000;

            var global = Visitor.Global as JsObject;

            global["ToBoolean"] = Visitor.Global.FunctionClass.New(new Func<object, Boolean>(Convert.ToBoolean));
            global["ToByte"] = Visitor.Global.FunctionClass.New(new Func<object, Byte>(Convert.ToByte));
            global["ToChar"] = Visitor.Global.FunctionClass.New(new Func<object, Char>(Convert.ToChar));
            global["ToDateTime"] = Visitor.Global.FunctionClass.New(new Func<object, DateTime>(Convert.ToDateTime));
            global["ToDecimal"] = Visitor.Global.FunctionClass.New(new Func<object, Decimal>(Convert.ToDecimal));
            global["ToDouble"] = Visitor.Global.FunctionClass.New(new Func<object, Double>(Convert.ToDouble));
            global["ToInt16"] = Visitor.Global.FunctionClass.New(new Func<object, Int16>(Convert.ToInt16));
            global["ToInt32"] = Visitor.Global.FunctionClass.New(new Func<object, Int32>(Convert.ToInt32));
            global["ToInt64"] = Visitor.Global.FunctionClass.New(new Func<object, Int64>(Convert.ToInt64));
            global["ToSByte"] = Visitor.Global.FunctionClass.New(new Func<object, SByte>(Convert.ToSByte));
            global["ToSingle"] = Visitor.Global.FunctionClass.New(new Func<object, Single>(Convert.ToSingle));
            global["ToString"] = Visitor.Global.FunctionClass.New(new Func<object, String>(Convert.ToString));
            global["ToUInt16"] = Visitor.Global.FunctionClass.New(new Func<object, UInt16>(Convert.ToUInt16));
            global["ToUInt32"] = Visitor.Global.FunctionClass.New(new Func<object, UInt32>(Convert.ToUInt32));
            global["ToUInt64"] = Visitor.Global.FunctionClass.New(new Func<object, UInt64>(Convert.ToUInt64));

            BreakPoints = new List<BreakPoint>();
        }

        /// <summary>
        /// A global object associated with this engine instance
        /// </summary>
        public IGlobal Global {
            get { return Visitor.Global; }
        }

        private bool allowClr = false;
        private PermissionSet permissionSet;

        public static Program Compile(string source, bool debugInformation) {
            Program program = null;
            if (!string.IsNullOrEmpty(source)) {
                var lexer = new ES3Lexer(new ANTLRStringStream(source));
                var parser = new ES3Parser(new CommonTokenStream(lexer)) { DebugMode = debugInformation };

                program = parser.program().value;

                if (parser.Errors != null && parser.Errors.Count > 0) {
                    throw new JintException(String.Join(Environment.NewLine, parser.Errors.ToArray()));
                }
            }

            return program;
        }

        /// <summary>
        /// Pre-compiles the expression in order to check syntax errors.
        /// If errors are detected, the Error property contains the message.
        /// </summary>
        /// <returns>True if the expression syntax is correct, otherwiser False</returns>
        public static bool HasErrors(string script, out string errors) {
            try {
                errors = null;
                Program program = Compile(script, false);

                // In case HasErrors() is called multiple times for the same expression
                return program != null;
            }
            catch (Exception e) {
                errors = e.Message;
                return true;
            }
        }

        /// <summary>
        /// Runs a set of JavaScript statements and optionally returns a value if return is called
        /// </summary>
        /// <param name="script">The script to execute</param>
        /// <returns>Optionaly, returns a value from the scripts</returns>
        /// <exception cref="System.ArgumentException" />
        /// <exception cref="System.Security.SecurityException" />
        /// <exception cref="Jint.JintException" />
        public object Run(string script) {
            return Run(script, true);
        }

        /// <summary>
        /// Runs a set of JavaScript statements and optionally returns a value if return is called
        /// </summary>
        /// <param name="program">The expression tree to execute</param>
        /// <returns>Optionaly, returns a value from the scripts</returns>
        /// <exception cref="System.ArgumentException" />
        /// <exception cref="System.Security.SecurityException" />
        /// <exception cref="Jint.JintException" />
        public object Run(Program program) {
            return Run(program, true);
        }

        /// <summary>
        /// Runs a set of JavaScript statements and optionally returns a value if return is called
        /// </summary>
        /// <param name="reader">The TextReader to read script from</param>
        /// <returns>Optionaly, returns a value from the scripts</returns>
        /// <exception cref="System.ArgumentException" />
        /// <exception cref="System.Security.SecurityException" />
        /// <exception cref="Jint.JintException" />
        public object Run(TextReader reader) {
            return Run(reader.ReadToEnd());
        }

        /// <summary>
        /// Runs a set of JavaScript statements and optionally returns a value if return is called
        /// </summary>
        /// <param name="reader">The TextReader to read script from</param>
        /// <param name="unwrap">Whether to unwrap the returned value to a CLR instance. <value>True</value> by default.</param>
        /// <returns>Optionaly, returns a value from the scripts</returns>
        /// <exception cref="System.ArgumentException" />
        /// <exception cref="System.Security.SecurityException" />
        /// <exception cref="Jint.JintException" />
        public object Run(TextReader reader, bool unwrap) {
            return Run(reader.ReadToEnd(), unwrap);
        }

        /// <summary>
        /// Runs a set of JavaScript statements and optionally returns a value if return is called
        /// </summary>
        /// <param name="script">The script to execute</param>
        /// <param name="unwrap">Whether to unwrap the returned value to a CLR instance. <value>True</value> by default.</param>
        /// <returns>Optionaly, returns a value from the scripts</returns>
        /// <exception cref="System.ArgumentException" />
        /// <exception cref="System.Security.SecurityException" />
        /// <exception cref="Jint.JintException" />
        public object Run(string script, bool unwrap) {

            if (script == null)
                throw new
                    ArgumentException("Script can't be null", "script");

            Program program;



            try {
                program = Compile(script, DebugMode);
            }
            catch (Exception e) {
                throw new JintException("An unexpected error occured while parsing the script", e);
            }

            if (program == null)
                return null;

            return Run(program, unwrap);
        }

		public void ResetSteps()
		{
			Visitor.ResetSteps();
		}

        /// <summary>
        /// Runs a set of JavaScript statements and optionally returns a value if return is called
        /// </summary>
        /// <param name="program">The expression tree to execute</param>
        /// <param name="unwrap">Whether to unwrap the returned value to a CLR instance. <value>True</value> by default.</param>
        /// <returns>Optionaly, returns a value from the scripts</returns>
        /// <exception cref="System.ArgumentException" />
        /// <exception cref="System.Security.SecurityException" />
        /// <exception cref="Jint.JintException" />
        public object Run(Program program, bool unwrap) {
            if (program == null)
                throw new
                    ArgumentException("Script can't be null", "script");

            Visitor.DebugMode = this.DebugMode;
            Visitor.MaxRecursions = this.MaxRecursions;
	        Visitor.MaxSteps = this.MaxSteps;
            Visitor.PermissionSet = permissionSet;
            Visitor.AllowClr = allowClr;
            Visitor.Result = null;
	        Visitor.ResetSteps();

            if (DebugMode) {
                Visitor.Step += OnStep;
            }

            try {
                Visitor.Visit(program);
            }
            catch (SecurityException) {
                throw;
            }
            catch (JsException e) {
                string message = e.Message;
                if (e.Value is JsError)
                    message = e.Value.Value.ToString();
                var stackTrace = new StringBuilder();
                var source = String.Empty;

                if (DebugMode) {
                    while (Visitor.CallStack.Count > 0) {
                        stackTrace.AppendLine(Visitor.CallStack.Pop());
                    }

                    if (stackTrace.Length > 0) {
                        stackTrace.Insert(0, Environment.NewLine + "------ Stack Trace:" + Environment.NewLine);
                    }
                }

                if (Visitor.CurrentStatement.Source != null) {
                    source = Environment.NewLine + Visitor.CurrentStatement.Source.ToString()
                            + Environment.NewLine + Visitor.CurrentStatement.Source.Code;
                }

                throw new JintException(message + source + stackTrace, e);
            }
            catch (Exception e) {
                StringBuilder stackTrace = new StringBuilder();
                string source = String.Empty;

                if (DebugMode) {
                    while (Visitor.CallStack.Count > 0) {
                        stackTrace.AppendLine(Visitor.CallStack.Pop());
                    }

                    if (stackTrace.Length > 0) {
                        stackTrace.Insert(0, Environment.NewLine + "------ Stack Trace:" + Environment.NewLine);
                    }
                }

                if (Visitor.CurrentStatement != null && Visitor.CurrentStatement.Source != null) {
                    source = Environment.NewLine + Visitor.CurrentStatement.Source.ToString()
                            + Environment.NewLine + Visitor.CurrentStatement.Source.Code;
                }

                throw new JintException(e.Message + source + stackTrace, e);
            }
            finally {
                Visitor.Step -= OnStep;
            }

            return Visitor.Result == null ? null : unwrap ? Visitor.Global.Marshaller.MarshalJsValue<object>( Visitor.Result) : Visitor.Result;
        }

        #region Debugger
        public event EventHandler<DebugInformation> Step;
        public event EventHandler<DebugInformation> Break;
        public List<BreakPoint> BreakPoints { get; private set; }
        public bool DebugMode { get; private set; }
        public int MaxRecursions { get; private set; }
		public int MaxSteps { get; private set; }
        public List<string> WatchList { get; set; }

        public JintEngine SetDebugMode(bool debugMode) {
            DebugMode = debugMode;
            return this;
        }

        /// <summary>
        /// Defines the max allowed number of recursions in the script
        /// </summary>
        public JintEngine SetMaxRecursions(int maxRecursions) {
            MaxRecursions = maxRecursions;
            return this;
        }


		/// <summary>
		/// Defines the max allowed number of steps in the script
		/// </summary>
		public JintEngine SetMaxSteps(int maxSteps)
		{
			MaxSteps = maxSteps;
			return this;
		}

        #endregion

		public object GetParameter(string name)
		{
			var jsInstance = Visitor.GlobalScope[name];
			return Visitor.Global.Marshaller.MarshalJsValue<object>(jsInstance);
		}

		public void RemoveParameter(string name)
		{
			Visitor.GlobalScope.Delete(name);
		}

        #region SetParameter overloads

        /// <summary>
        /// Defines an external object to be available inside the script
        /// </summary>
        /// <param name="name">Local name of the object duting the execution of the script</param>
        /// <param name="value">Available object</param>
        /// <returns>The current JintEngine instance</returns>
        public JintEngine SetParameter(string name, object value) {
            Visitor.GlobalScope[name] = Visitor.Global.WrapClr(value);
            return this;
        }

        /// <summary>
        /// Defines an external Double value to be available inside the script
        /// </summary>
        /// <param name="name">Local name of the Double value during the execution of the script</param>
        /// <param name="value">Available Double value</param>
        /// <returns>The current JintEngine instance</returns>
        public JintEngine SetParameter(string name, double value) {
            Visitor.GlobalScope[name] = Visitor.Global.NumberClass.New(value);
            return this;
        }


		/// <summary>
		/// Defines an external JsObject value to be available inside the script
		/// </summary>
		/// <param name="name">Local name of the Double value during the execution of the script</param>
		/// <param name="value">Available JsObject value</param>
		/// <returns>The current JintEngine instance</returns>
		public JintEngine SetParameter(string name, JsObject value)
		{
			Visitor.GlobalScope[name] = value;
			return this;
		}

        /// <summary>
        /// Defines an external String instance to be available inside the script
        /// </summary>
        /// <param name="name">Local name of the String instance during the execution of the script</param>
        /// <param name="value">Available String instance</param>
        /// <returns>The current JintEngine instance</returns>
        public JintEngine SetParameter(string name, string value) {
            if (value == null)
                Visitor.GlobalScope[name] = JsNull.Instance;
            else
                Visitor.GlobalScope[name] = Visitor.Global.StringClass.New(value);
            return this;
        }

        /// <summary>
        /// Defines an external Int32 value to be available inside the script
        /// </summary>
        /// <param name="name">Local name of the Int32 value during the execution of the script</param>
        /// <param name="value">Available Int32 value</param>
        /// <returns>The current JintEngine instance</returns>
        public JintEngine SetParameter(string name, int value) {
            Visitor.GlobalScope[name] = Visitor.Global.WrapClr(value);
            return this;
        }

        /// <summary>
        /// Defines an external Boolean value to be available inside the script
        /// </summary>
        /// <param name="name">Local name of the Boolean value during the execution of the script</param>
        /// <param name="value">Available Boolean value</param>
        /// <returns>The current JintEngine instance</returns>
        public JintEngine SetParameter(string name, bool value) {
            Visitor.GlobalScope[name] = Visitor.Global.BooleanClass.New(value);
            return this;
        }

        /// <summary>
        /// Defines an external DateTime value to be available inside the script
        /// </summary>
        /// <param name="name">Local name of the DateTime value during the execution of the script</param>
        /// <param name="value">Available DateTime value</param>
        /// <returns>The current JintEngine instance</returns>
        public JintEngine SetParameter(string name, DateTime value) {
            Visitor.GlobalScope[name] = Visitor.Global.DateClass.New(value);
            return this;
        }
        #endregion

        public JintEngine AddPermission(IPermission perm) {
            permissionSet.AddPermission(perm);
            return this;
        }

        public JintEngine SetFunction(string name, JsFunction function) {
            Visitor.GlobalScope[name] = function;
            return this;
        }

        public object CallFunction(string name, params object[] args) {
            JsInstance oldResult = Visitor.Result;
            Visitor.Visit(new Identifier(name));
            var returnValue = CallFunction((JsFunction)Visitor.Result, args);
            Visitor.Result = oldResult;
            return returnValue;
        }

        public object CallFunction(JsFunction function, params object[] args) {
            Visitor.ExecuteFunction(function, null, Array.ConvertAll<object,JsInstance>( args, x => Visitor.Global.Marshaller.MarshalClrValue<object>(x) ));
            return Visitor.Global.Marshaller.MarshalJsValue<object>(Visitor.Returned);
        }

        public JintEngine SetFunction(string name, Delegate function) {
            Visitor.GlobalScope[name] = Visitor.Global.FunctionClass.New(function);
            return this;
        }

        /// <summary>
        /// Escapes a JavaScript string literal
        /// </summary>
        /// <param name="value">The string literal to espace</param>
        /// <returns>The escaped string literal, without sinlge quotes, back slashes and line breaks</returns>
        public static string EscapteStringLiteral(string value) {
            return value.Replace("\\", "\\\\").Replace("'", "\\'").Replace(Environment.NewLine, "\\r\\n");
        }

        protected void OnStep(object sender, DebugInformation info) {
            if (Step != null) {
                Step(this, info);
            }

            if (Break != null) {
                BreakPoint breakpoint = BreakPoints.Find(l => {
                    bool afterStart, beforeEnd;

                    afterStart = l.Line > info.CurrentStatement.Source.Start.Line
                        || (l.Line == info.CurrentStatement.Source.Start.Line && l.Char >= info.CurrentStatement.Source.Start.Char);

                    if (!afterStart) {
                        return false;
                    }

                    beforeEnd = l.Line < info.CurrentStatement.Source.Stop.Line
                        || (l.Line == info.CurrentStatement.Source.Stop.Line && l.Char <= info.CurrentStatement.Source.Stop.Char);

                    if (!beforeEnd) {
                        return false;
                    }

                    if (!String.IsNullOrEmpty(l.Condition)) {
                        return Convert.ToBoolean(this.Run(l.Condition));
                    }

                    return true;
                });


                if (breakpoint != null) {
                    Break(this, info);
                }
            }
        }

        protected void OnBreak(object sender, DebugInformation info) {
            if (Break != null) {
                Break(this, info);
            }
        }

        public JintEngine DisableSecurity() {
            permissionSet = new PermissionSet(PermissionState.Unrestricted);
            return this;
        }

        public JintEngine AllowClr()
        {
            allowClr = true;
            return this;
        }

        public JintEngine AllowClr(bool value)
        {
            allowClr = value;
            return this;
        }

        public JintEngine EnableSecurity()
        {
            permissionSet = new PermissionSet(PermissionState.None);
            return this;
        }

        public void Save(Stream s) {
            BinaryFormatter formatter = new BinaryFormatter();
            formatter.Serialize(s, Visitor);
        }

        public static void Load(JintEngine engine, Stream s) {
            BinaryFormatter formatter = new BinaryFormatter();
            var visitor = (ExecutionVisitor)formatter.Deserialize(s);
            engine.Visitor = visitor;
        }

        public static JintEngine Load(Stream s) {
            JintEngine engine = new JintEngine();
            Load(engine, s);
            return engine;
        }
    }
}
