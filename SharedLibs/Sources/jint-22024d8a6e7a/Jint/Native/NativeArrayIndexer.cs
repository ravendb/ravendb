using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native
{
    class NativeArrayIndexer<T>: INativeIndexer
    {
        Marshaller m_marshller;
        public NativeArrayIndexer(Marshaller marshaller)
        {
            if (marshaller == null)
                throw new ArgumentNullException("marshaller");
            m_marshller = marshaller;
        }
        #region INativeIndexer Members

        public JsInstance get(JsInstance that, JsInstance index)
        {
            return m_marshller.MarshalClrValue<T>( m_marshller.MarshalJsValue<T[]>(that)[m_marshller.MarshalJsValue<int>(index)] );
        }

        public void set(JsInstance that, JsInstance index, JsInstance value)
        {
            m_marshller.MarshalJsValue<T[]>(that)[m_marshller.MarshalJsValue<int>(index)] = m_marshller.MarshalJsValue<T>(value);
        }

        #endregion
    }
}
