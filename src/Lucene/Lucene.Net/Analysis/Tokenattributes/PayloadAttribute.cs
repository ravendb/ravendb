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
using Attribute = Lucene.Net.Util.Attribute;
using Payload = Lucene.Net.Index.Payload;

namespace Lucene.Net.Analysis.Tokenattributes
{

    /// <summary> The payload of a Token. See also <see cref="Payload" />.</summary>
    [Serializable]
    public sealed class PayloadAttribute:Attribute, IPayloadAttribute, System.ICloneable
	{
		private Payload payload;
		
		/// <summary> Initialize this attribute with no payload.</summary>
		public PayloadAttribute()
		{
		}
		
		/// <summary> Initialize this attribute with the given payload. </summary>
		public PayloadAttribute(Payload payload)
		{
			this.payload = payload;
		}

	    /// <summary> Returns this Token's payload.</summary>
	    public Payload Payload
	    {
	        get { return this.payload; }
	        set { this.payload = value; }
	    }

	    public override void  Clear()
		{
			payload = null;
		}
		
		public override System.Object Clone()
		{
		    var clone = (PayloadAttribute) base.Clone();
            if (payload != null)
            {
                clone.payload = (Payload) payload.Clone();
            }
		    return clone;
            // TODO: This code use to be as below.  Any reason why?  the if(payload!=null) was missing...
		    //PayloadAttributeImpl impl = new PayloadAttributeImpl();
		    //impl.payload = new Payload(this.payload.data, this.payload.offset, this.payload.length);
		    //return impl;
		}
		
		public  override bool Equals(System.Object other)
		{
			if (other == this)
			{
				return true;
			}
			
			if (other is IPayloadAttribute)
			{
				PayloadAttribute o = (PayloadAttribute) other;
				if (o.payload == null || payload == null)
				{
					return o.payload == null && payload == null;
				}
				
				return o.payload.Equals(payload);
			}
			
			return false;
		}
		
		public override int GetHashCode()
		{
			return (payload == null)?0:payload.GetHashCode();
		}
		
		public override void  CopyTo(Attribute target)
		{
			IPayloadAttribute t = (IPayloadAttribute) target;
			t.Payload = (payload == null)?null:(Payload) payload.Clone();
		}
	}
}