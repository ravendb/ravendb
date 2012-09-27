using System;
using System.Collections.Generic;
using System.Text;
using Jint.Expressions;

namespace Jint.Native {
    [Serializable]
    public class JsFunction : JsObject {
        public static string CALL = "call";
        public static string APPLY = "apply";
        public static string CONSTRUCTOR = "constructor";
        public static string PROTOTYPE = "prototype";

        public string Name { get; set; }
        public Statement Statement { get; set; }
        public List<string> Arguments { get; set; }
        public JsScope Scope { get; set; }

        public JsFunction(IGlobal global, Statement statement)
            : this(global.FunctionClass.PrototypeProperty) {
            Statement = statement;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="global"></param>
        public JsFunction(IGlobal global)
            : this(global.FunctionClass.PrototypeProperty) {
        }

        /// <summary>
        /// Init new function object with a specified prototype
        /// </summary>
        /// <param name="prototype">prototype for this object</param>
        public JsFunction(JsObject prototype)
            : base(prototype) {
            Arguments = new List<string>();
            Statement = new EmptyStatement();
            DefineOwnProperty(PROTOTYPE, JsNull.Instance, PropertyAttributes.DontEnum);
        }

        public override int Length
        {
            get
            {
                return Arguments.Count;
            }
            set
            {
                ;
            }
        }

        public JsObject PrototypeProperty {
            get {
                return this[PROTOTYPE] as JsObject;
            }
            set {
                this[PROTOTYPE] = value;
            }
        }

        //15.3.5.3
        public virtual bool HasInstance(JsObject inst) {
            if (inst != null && inst != JsNull.Instance && inst != JsNull.Instance) {
                return this.PrototypeProperty.IsPrototypeOf(inst);
            }
            return false;
        }

        //13.2.2
        public virtual JsObject Construct(JsInstance[] parameters, Type[] genericArgs, IJintVisitor visitor) {
            var instance = visitor.Global.ObjectClass.New(PrototypeProperty);
            visitor.ExecuteFunction(this, instance, parameters);

            return (visitor.Result as JsObject ?? instance);
        }

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        public override object Value {
            get { return null; }
            set { }
        }

        public virtual JsInstance Execute(IJintVisitor visitor, JsDictionaryObject that, JsInstance[] parameters) {
            Statement.Accept((IStatementVisitor)visitor);
            return that;
        }

        public virtual JsInstance Execute(IJintVisitor visitor, JsDictionaryObject that, JsInstance[] parameters, Type[] genericArguments)
        {
            throw new JintException("This method can't be called as a generic");
        }

        public override string Class {
            get { return CLASS_FUNCTION; }
        }

        public override string ToSource() {
            return String.Format("function {0} ( {1} ) {{ {2} }}", Name, String.Join(", ", Arguments.ToArray()), GetBody());
        }

        public virtual string GetBody() {
            return "/* js code */";
        }

        public override string ToString()
        {
            return ToSource();
        }

        public override bool ToBoolean() {
            return true;
        }

        public override double ToNumber() {
            return 1;
        }
    }
}
