using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native
{
    /// <summary>
    /// This class is used to reflect a native generic.
    /// </summary>
    class NativeGenericType: JsObject
    {
        Type m_reflectedType;

        public NativeGenericType(Type reflectedType, JsObject prototype)
            : base(prototype)
        {
            if (reflectedType == null)
                throw new ArgumentNullException("reflectedType");
        }

        public override object Value
        {
            get
            {
                return m_reflectedType;
            }
            set
            {
                m_reflectedType = (Type)value;
            }
        }

        JsConstructor MakeType(Type[] args, IGlobal global)
        {
            return global.Marshaller.MarshalType( m_reflectedType.MakeGenericType(args) );
        }
    }
}
