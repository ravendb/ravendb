using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using Jint;
using Jint.Marshal;

namespace Jint.Native
{
    /// <summary>
    /// Descriptor which get and set methods are implemented through delegates
    /// </summary>
    public class NativeDescriptor : Descriptor
    {
        public NativeDescriptor(JsDictionaryObject owner, string name, JsGetter getter)
            : base(owner, name)
        {
            this.getter = getter;
            Writable = false;
        }

        public NativeDescriptor(JsDictionaryObject owner, string name, JsGetter getter, JsSetter setter)
            : base(owner, name)
        {
            this.getter = getter;
            this.setter = setter;
        }

        public NativeDescriptor(JsDictionaryObject owner, NativeDescriptor src)
            : base(owner, src.Name)
        {
            getter = src.getter;
            setter = src.setter;
            Writable = src.Writable;
            Configurable = src.Configurable;
            Enumerable = src.Enumerable;
        }

        JsGetter getter;
        JsSetter setter;

        public override bool isReference {
            get { return false; }
        }

        public override Descriptor Clone() {
            return new NativeDescriptor(Owner, this);
        }

        public override JsInstance Get(JsDictionaryObject that)
        {
            return getter != null ? getter(that) : JsUndefined.Instance ;
        }

        public override void Set(JsDictionaryObject that, JsInstance value)
        {
            if (setter != null)
                setter(that, value);
        }

        internal override DescriptorType DescriptorType
        {
            get { return DescriptorType.Clr; }
        }


    }
}
