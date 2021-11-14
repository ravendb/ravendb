using System;
using System.Collections.Generic;
using V8.Net;

namespace Raven.Server.Documents.Patch.V8
{
    public class DictionaryCloningKeyIHV8<TValue> : DictionaryCloningKey<InternalHandle, TValue>
    {
    }

    public class DictionaryCloningValueIHV8<TKey> : DictionaryCloningValue<TKey, InternalHandle>
    {
    }
}
