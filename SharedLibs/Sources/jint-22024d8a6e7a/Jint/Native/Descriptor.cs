using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native {
    internal enum DescriptorType {
        Value,
        Accessor,
        Clr
    }

    [Serializable]
    public abstract class Descriptor {
        public Descriptor(JsDictionaryObject owner, string name) {
            this.Owner = owner;
            Name = name;
        }

        public string Name { get; set; }

        public bool Enumerable { get; set; }
        public bool Configurable { get; set; }
        public bool Writable { get; set; }
        public JsDictionaryObject Owner { get; set; }

        public virtual bool isDeleted { get; protected set; }

        public abstract bool isReference { get; }

        /// <summary>
        /// Marks a descriptor as a deleted.
        /// </summary>
        /// <remarks>
        /// A descriptor may be deleted to force a refresh of cached references.
        /// </remarks>
        public virtual void Delete() {
            isDeleted = true;
        }

        public bool IsClr {
            get { return false; }
        }

        public abstract Descriptor Clone();

        /// <summary>
        /// Gets a value stored in the descriptor.
        /// </summary>
        /// <param name="that">A target object. This has a meaning in case of descriptors which helds an accessors,
        /// in value descriptors this parameter is ignored.</param>
        /// <returns>A value stored in the descriptor</returns>
        public abstract JsInstance Get(JsDictionaryObject that);

        /// <summary>
        /// Sets a value.
        /// </summary>
        /// <param name="that">A target object. This has a meaning in case of descriptors which helds an accessors,
        /// in value descriptors this parameter is ignored.</param>
        /// <param name="value">A new value which should be stored in the descriptor.</param>
        public abstract void Set(JsDictionaryObject that, JsInstance value);

        internal abstract DescriptorType DescriptorType { get; }

        /// <summary>
        /// 8.10.5
        /// </summary>
        /// <param name="global"></param>
        /// <param name="obj"></param>
        /// <returns></returns>
        internal static Descriptor ToPropertyDesciptor(IGlobal global, JsDictionaryObject owner, string name, JsInstance jsInstance) {
            if (jsInstance.Class != JsInstance.CLASS_OBJECT) {
                throw new JsException(global.TypeErrorClass.New("The target object has to be an instance of an object"));
            }

            JsObject obj = (JsObject)jsInstance;
            if ((obj.HasProperty("value") || obj.HasProperty("writable")) && (obj.HasProperty("set") || obj.HasProperty("get"))) {
                throw new JsException(global.TypeErrorClass.New("The property cannot be both writable and have get/set accessors or cannot have both a value and an accessor defined"));
            }

            Descriptor desc;
            JsInstance result = null;

            if (obj.HasProperty("value")) {
                desc = new ValueDescriptor(owner, name, obj["value"]);
            }
            else {
                desc = new PropertyDescriptor(global, owner, name);
            }

            if (obj.TryGetProperty("enumerable", out result)) {
                desc.Enumerable = result.ToBoolean();
            }

            if (obj.TryGetProperty("configurable", out result)) {
                desc.Configurable = result.ToBoolean();
            }

            if (obj.TryGetProperty("writable", out result)) {
                desc.Writable = result.ToBoolean();
            }

            if (obj.TryGetProperty("get", out result)) {
                if (!(result is JsFunction))
                    throw new JsException(global.TypeErrorClass.New("The getter has to be a function"));

                ((PropertyDescriptor)desc).GetFunction = (JsFunction)result;
            }

            if (obj.TryGetProperty("set", out result)) {
                if (!(result is JsFunction))
                    throw new JsException(global.TypeErrorClass.New("The setter has to be a function"));

                ((PropertyDescriptor)desc).SetFunction = (JsFunction)result;
            }

            return desc;
        }

		public override string ToString()
		{
			return Name;
		}
    }
}
