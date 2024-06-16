/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Runtime.CompilerServices;
using Attribute = Lucene.Net.Util.Attribute;

namespace Lucene.Net.Analysis.Tokenattributes
{

    /// <summary> A Token's lexical type. The Default value is "word". </summary>
    [Serializable]
    public sealed class TypeAttribute:Attribute, ITypeAttribute, System.ICloneable
	{
		private System.String type;
		public const System.String DEFAULT_TYPE = "word";
		
		public TypeAttribute():this(DEFAULT_TYPE)
		{
		}
		
		public TypeAttribute(System.String type)
		{
			this.type = type;
		}

	    /// <summary>Returns this Token's lexical type.  Defaults to "word". </summary>
	    public string Type
	    {
	        get { return type; }
	        set { this.type = value; }
	    }

	    [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Clear()
        {
            type = DEFAULT_TYPE;
        }

	    // PERF: When CoreCLR 2.0 this can be replaced for Clear with AggresiveInlining because of devirtualization.
	    [MethodImpl(MethodImplOptions.AggressiveInlining)]
	    public void ClearFast()
	    {
	        type = DEFAULT_TYPE;
	    }

        public  override bool Equals(System.Object other)
		{
			if (other == this)
			{
				return true;
			}
			
			if (other is TypeAttribute)
			{
				return type.Equals(((TypeAttribute) other).type);
			}
			
			return false;
		}
		
		public override int GetHashCode()
		{
			return type.GetHashCode();
		}
		
		public override void  CopyTo(Attribute target)
		{
			ITypeAttribute t = (ITypeAttribute) target;
			t.Type = type;
		}
		
		override public System.Object Clone()
		{
            TypeAttribute impl = new TypeAttribute();
            impl.type = type;
            return impl;
		}
	}
}