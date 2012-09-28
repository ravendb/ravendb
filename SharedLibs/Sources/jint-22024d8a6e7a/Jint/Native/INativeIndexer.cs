using System;
namespace Jint.Native
{
    public interface INativeIndexer
    {
        JsInstance get(JsInstance that, JsInstance index);
        void set(JsInstance that, JsInstance index, JsInstance value);
    }
}
