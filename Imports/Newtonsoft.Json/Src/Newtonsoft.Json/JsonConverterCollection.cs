#region License
// Copyright (c) 2007 James Newton-King
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace Raven.Imports.Newtonsoft.Json
{
    /// <summary>
    /// Represents a collection of <see cref="JsonConverter"/>.
    /// </summary>
    public class JsonConverterCollection : Collection<JsonConverter>
    {
        public static readonly JsonConverterCollection Empty = new JsonConverterCollection();

        static JsonConverterCollection()
        {
            Empty.Freeze();
        }

        public JsonConverterCollection()
        {
            this.IsFrozen = false;
        }

        public JsonConverterCollection(IEnumerable<JsonConverter> converters)
        {
            this.IsFrozen = false;

            if (converters != null)
            {
                foreach (var item in converters)
                    this.Add(item);
            }
        }

        protected JsonConverterCollection( JsonConverterCollection collection ) : base ( collection )
        {
            this.IsFrozen = collection.IsFrozen;
        }

        protected override void ClearItems()
        {
            if (IsFrozen)
                throw new InvalidOperationException("Cannot modify a frozen collection.");

            base.ClearItems();
        }

        protected override void InsertItem(int index, JsonConverter item)
        {
            if (IsFrozen)
                throw new InvalidOperationException("Cannot modify a frozen collection.");

            base.InsertItem(index, item);
        }

        protected override void RemoveItem(int index)
        {
            if (IsFrozen)
                throw new InvalidOperationException("Cannot modify a frozen collection.");

            base.RemoveItem(index);
        }

        protected override void SetItem(int index, JsonConverter item)
        {
            if (IsFrozen)
                throw new InvalidOperationException("Cannot modify a frozen collection.");

            base.SetItem(index, item);
        }

        public bool IsFrozen
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        public void Freeze()
        {
            this.IsFrozen = true;
        }

    }
}
