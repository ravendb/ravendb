using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native {
    /// <summary>
    /// Scope. Uses Prototype inheritance to store scopes hierarchy.
    /// </summary>
    /// <remarks>
    /// Tries to add new properties to the global scope.
    /// </remarks>
    [Serializable]
    public class JsScope : JsDictionaryObject {
        private Descriptor thisDescriptor;
        private Descriptor argumentsDescriptor;
        private JsScope globalScope;
        private JsDictionaryObject bag;

        public static string THIS = "this";
        public static string ARGUMENTS = "arguments";

        /// <summary>
        /// Creates a new Global scope
        /// </summary>
        public JsScope()
            : base(JsNull.Instance) {
            globalScope = null;
        }

        /// <summary>
        /// Creates a new scope inside the specified scope
        /// </summary>
        /// <param name="outer">Scope inside which the new scope should be created</param>
        public JsScope(JsScope outer)
            : base(outer) {
            if (outer == null)
                throw new ArgumentNullException("outer");

            globalScope = outer.Global;
        }

        public JsScope(JsScope outer, JsDictionaryObject bag)
            : base(outer) {
            if (outer == null)
                throw new ArgumentNullException("outer");
            if (bag == null)
                throw new ArgumentNullException("bag");
            globalScope = outer.Global;
            this.bag = bag;
        }

        public JsScope(JsDictionaryObject bag)
            : base(JsNull.Instance) {
            this.bag = bag;
        }

        public override string Class {
            get { return CLASS_SCOPE; }
        }

        public override string Type
        {
            get { return TYPE_OBJECT; }
        }

        public JsScope Global {
            get { return globalScope ?? this; }
        }

        public override JsInstance this[string index] {
            get {
                if (index == THIS && thisDescriptor != null)
                    return thisDescriptor.Get(this);
                if (index == ARGUMENTS && argumentsDescriptor != null)
                    return argumentsDescriptor.Get(this);
                return base[index]; // will use overriden GetDescriptor
            }
            set {
                if (index == THIS) {
                    if (thisDescriptor != null)
                        thisDescriptor.Set(this, value);
                    else {
                        DefineOwnProperty(thisDescriptor = new ValueDescriptor(this, index, value));
                    }
                }
                else if (index == ARGUMENTS) {
                    if (argumentsDescriptor != null)
                        argumentsDescriptor.Set(this, value);
                    else {
                        DefineOwnProperty(argumentsDescriptor = new ValueDescriptor(this, index, value));
                    }
                }
                else {
                    Descriptor d = GetDescriptor(index);
                    if (d != null) {
                        d.Set(this, value);
                    }
                    else if (globalScope != null) {
                        // TODO: move to Execution visitor
                        // define missing property in the global scope
                        globalScope.DefineOwnProperty(index, value);
                    }
                    else {
                        // this scope is a global scope
                        DefineOwnProperty(index, value);
                    }
                }
            }
        }

        /// <summary>
        /// Overriden. Returns a property descriptor.
        /// </summary>
        /// <remarks>
        /// Tries to resolve proeprty in the following order:
        /// 
        /// 1. OwnProperty for the current scope
        /// 2. Any property from the bag (if specified).
        /// 3. A property from scopes hierarchy.
        /// 
        /// A proeprty from the bag will be added as a link to the current scope.
        /// </remarks>
        /// <param name="index">Property name.</param>
        /// <returns>Descriptor</returns>
        public override Descriptor GetDescriptor(string index) {
            Descriptor own, d;
            if ((own = base.GetDescriptor(index)) != null && own.Owner == this)
                return own;

            if (bag != null && (d = bag.GetDescriptor(index)) != null) {
                Descriptor link = new LinkedDescriptor(this, d.Name, d, bag);
                DefineOwnProperty(link);
                return link;
            }

            return own;
        }

        public override void DefineOwnProperty(string key, JsInstance value) {
            if (bag != null) {
                DefineOwnProperty(new ValueDescriptor(bag, key, value));
            } 
            else {
                DefineOwnProperty(new ValueDescriptor(this, key, value));
            }
        }

        public override void DefineOwnProperty(Descriptor currentDescriptor) {
            if (bag != null) {
                bag.DefineOwnProperty(currentDescriptor);
            }
            else {
                base.DefineOwnProperty(currentDescriptor);
            }
        }

        public override bool HasOwnProperty(string key) {
            if (bag != null) {
                return bag.HasOwnProperty(key);
            }
            else {
                return base.HasOwnProperty(key);
            }
        }

        public override IEnumerable<string> GetKeys() {
            if (bag != null) {
                foreach (var key in bag.GetKeys())
                    if (baseGetDescriptor(key) == null)
                        yield return key;
            }
            foreach (var key in baseGetKeys())
                yield return key;
        }

        private Descriptor baseGetDescriptor(string key)
        {
            return base.GetDescriptor(key);
        }

        private IEnumerable<string> baseGetKeys()
        {
            return base.GetKeys();
        }

        public override IEnumerable<JsInstance> GetValues() {
            foreach (var key in GetKeys())
                yield return this[key];
        }

        public override bool IsClr {
            get {
                return bag != null ? bag.IsClr : false;
            }
        }

        public override object Value {
            get {
                return bag != null ? bag.Value : null;
            }
            set {
                if (bag != null)
                    bag.Value = value;
            }
        }

    }
}
