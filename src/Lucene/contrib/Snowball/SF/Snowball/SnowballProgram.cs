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
using System.Text;

namespace SF.Snowball
{
	/// <summary>
	/// This is the rev 500 of the snowball SVN trunk,
	/// but modified:
	/// made abstract and introduced abstract method stem to avoid expensive reflection in filter class
	/// </summary>
	public abstract class SnowballProgram
	{
		protected internal SnowballProgram()
		{
			current = new System.Text.StringBuilder();
			SetCurrent("");
		}

	    public abstract bool Stem();

		/// <summary> Set the current string.</summary>
		public virtual void  SetCurrent(System.String value)
		{
			//// current.Replace(current.ToString(0, current.Length - 0), value_Renamed, 0, current.Length - 0);
            current.Remove(0, current.Length);
            current.Append(value);
			cursor = 0;
			limit = current.Length;
			limit_backward = 0;
			bra = cursor;
			ket = limit;
		}

        /// <summary> Get the current string.</summary>
        virtual public System.String GetCurrent()
        {
            string result = current.ToString();
            // Make a new StringBuffer.  If we reuse the old one, and a user of
            // the library keeps a reference to the buffer returned (for example,
            // by converting it to a String in a way which doesn't force a copy),
            // the buffer size will not decrease, and we will risk wasting a large
            // amount of memory.
            // Thanks to Wolfram Esser for spotting this problem.
            current = new StringBuilder();
            return result;
        }

		// current string
		protected internal System.Text.StringBuilder current;
		
		protected internal int cursor;
		protected internal int limit;
		protected internal int limit_backward;
		protected internal int bra;
		protected internal int ket;
		
		protected internal virtual void  copy_from(SnowballProgram other)
		{
			current = other.current;
			cursor = other.cursor;
			limit = other.limit;
			limit_backward = other.limit_backward;
			bra = other.bra;
			ket = other.ket;
		}
		
		protected internal virtual bool in_grouping(char[] s, int min, int max)
		{
			if (cursor >= limit)
				return false;
			char ch = current[cursor];
			if (ch > max || ch < min)
				return false;
			ch -= (char) (min);
			if ((s[ch >> 3] & (0x1 << (ch & 0x7))) == 0)
				return false;
			cursor++;
			return true;
		}
		
		protected internal virtual bool in_grouping_b(char[] s, int min, int max)
		{
			if (cursor <= limit_backward)
				return false;
			char ch = current[cursor - 1];
			if (ch > max || ch < min)
				return false;
			ch -= (char) (min);
			if ((s[ch >> 3] & (0x1 << (ch & 0x7))) == 0)
				return false;
			cursor--;
			return true;
		}
		
		protected internal virtual bool out_grouping(char[] s, int min, int max)
		{
			if (cursor >= limit)
				return false;
			char ch = current[cursor];
			if (ch > max || ch < min)
			{
				cursor++;
				return true;
			}
			ch -= (char) (min);
			if ((s[ch >> 3] & (0x1 << (ch & 0x7))) == 0)
			{
				cursor++;
				return true;
			}
			return false;
		}
		
		protected internal virtual bool out_grouping_b(char[] s, int min, int max)
		{
			if (cursor <= limit_backward)
				return false;
			char ch = current[cursor - 1];
			if (ch > max || ch < min)
			{
				cursor--;
				return true;
			}
			ch -= (char) (min);
			if ((s[ch >> 3] & (0x1 << (ch & 0x7))) == 0)
			{
				cursor--;
				return true;
			}
			return false;
		}
		
		protected internal virtual bool in_range(int min, int max)
		{
			if (cursor >= limit)
				return false;
			char ch = current[cursor];
			if (ch > max || ch < min)
				return false;
			cursor++;
			return true;
		}
		
		protected internal virtual bool in_range_b(int min, int max)
		{
			if (cursor <= limit_backward)
				return false;
			char ch = current[cursor - 1];
			if (ch > max || ch < min)
				return false;
			cursor--;
			return true;
		}
		
		protected internal virtual bool out_range(int min, int max)
		{
			if (cursor >= limit)
				return false;
			char ch = current[cursor];
			if (!(ch > max || ch < min))
				return false;
			cursor++;
			return true;
		}
		
		protected internal virtual bool out_range_b(int min, int max)
		{
			if (cursor <= limit_backward)
				return false;
			char ch = current[cursor - 1];
			if (!(ch > max || ch < min))
				return false;
			cursor--;
			return true;
		}
		
		protected internal virtual bool eq_s(int s_size, System.String s)
		{
			if (limit - cursor < s_size)
				return false;
			int i;
			for (i = 0; i != s_size; i++)
			{
				if (current[cursor + i] != s[i])
					return false;
			}
			cursor += s_size;
			return true;
		}
		
		protected internal virtual bool eq_s_b(int s_size, System.String s)
		{
			if (cursor - limit_backward < s_size)
				return false;
			int i;
			for (i = 0; i != s_size; i++)
			{
				if (current[cursor - s_size + i] != s[i])
					return false;
			}
			cursor -= s_size;
			return true;
		}
		
		protected internal virtual bool eq_v(System.Text.StringBuilder s)
		{
			return eq_s(s.Length, s.ToString());
		}
		
		protected internal virtual bool eq_v_b(System.Text.StringBuilder s)
		{
			return eq_s_b(s.Length, s.ToString());
		}
		
		protected internal virtual int find_among(Among[] v, int v_size)
		{
			int i = 0;
			int j = v_size;
			
			int c = cursor;
			int l = limit;
			
			int common_i = 0;
			int common_j = 0;
			
			bool first_key_inspected = false;
			
			while (true)
			{
				int k = i + ((j - i) >> 1);
				int diff = 0;
				int common = common_i < common_j?common_i:common_j; // smaller
				Among w = v[k];
				int i2;
				for (i2 = common; i2 < w.s_size; i2++)
				{
					if (c + common == l)
					{
						diff = - 1;
						break;
					}
					diff = current[c + common] - w.s[i2];
					if (diff != 0)
						break;
					common++;
				}
				if (diff < 0)
				{
					j = k;
					common_j = common;
				}
				else
				{
					i = k;
					common_i = common;
				}
				if (j - i <= 1)
				{
					if (i > 0)
						break; // v->s has been inspected
					if (j == i)
						break; // only one item in v
					
					// - but now we need to go round once more to get
					// v->s inspected. This looks messy, but is actually
					// the optimal approach.
					
					if (first_key_inspected)
						break;
					first_key_inspected = true;
				}
			}
			while (true)
			{
				Among w = v[i];
				if (common_i >= w.s_size)
				{
					cursor = c + w.s_size;
					if (w.method == null)
						return w.result;
					bool res;
					try
					{
						System.Object resobj = w.method.Invoke(w.methodobject, (System.Object[]) new System.Object[0]);
						// {{Aroush}} UPGRADE_TODO: The equivalent in .NET for method 'java.lang.Object.toString' may return a different value. 'ms-help://MS.VSCC.2003/commoner/redir/redirect.htm?keyword="jlca1043_3"'
						res = resobj.ToString().Equals("true");
					}
					catch (System.Reflection.TargetInvocationException)
					{
						res = false;
						// FIXME - debug message
					}
					catch (System.UnauthorizedAccessException)
					{
						res = false;
						// FIXME - debug message
					}
					cursor = c + w.s_size;
					if (res)
						return w.result;
				}
				i = w.substring_i;
				if (i < 0)
					return 0;
			}
		}
		
		// find_among_b is for backwards processing. Same comments apply
		protected internal virtual int find_among_b(Among[] v, int v_size)
		{
			int i = 0;
			int j = v_size;
			
			int c = cursor;
			int lb = limit_backward;
			
			int common_i = 0;
			int common_j = 0;
			
			bool first_key_inspected = false;
			
			while (true)
			{
				int k = i + ((j - i) >> 1);
				int diff = 0;
				int common = common_i < common_j?common_i:common_j;
				Among w = v[k];
				int i2;
				for (i2 = w.s_size - 1 - common; i2 >= 0; i2--)
				{
					if (c - common == lb)
					{
						diff = - 1;
						break;
					}
					diff = current[c - 1 - common] - w.s[i2];
					if (diff != 0)
						break;
					common++;
				}
				if (diff < 0)
				{
					j = k;
					common_j = common;
				}
				else
				{
					i = k;
					common_i = common;
				}
				if (j - i <= 1)
				{
					if (i > 0)
						break;
					if (j == i)
						break;
					if (first_key_inspected)
						break;
					first_key_inspected = true;
				}
			}
			while (true)
			{
				Among w = v[i];
				if (common_i >= w.s_size)
				{
					cursor = c - w.s_size;
					if (w.method == null)
						return w.result;
					
					bool res;
					try
					{
						System.Object resobj = w.method.Invoke(w.methodobject, (System.Object[]) new System.Object[0]);
						// {{Aroush}} UPGRADE_TODO: The equivalent in .NET for method 'java.lang.Object.toString' may return a different value. 'ms-help://MS.VSCC.2003/commoner/redir/redirect.htm?keyword="jlca1043_3"'
						res = resobj.ToString().Equals("true");
					}
					catch (System.Reflection.TargetInvocationException)
					{
						res = false;
						// FIXME - debug message
					}
					catch (System.UnauthorizedAccessException)
					{
						res = false;
						// FIXME - debug message
					}
					cursor = c - w.s_size;
					if (res)
						return w.result;
				}
				i = w.substring_i;
				if (i < 0)
					return 0;
			}
		}
		
		/* to replace chars between c_bra and c_ket in current by the
		* chars in s.
		*/
		protected internal virtual int replace_s(int c_bra, int c_ket, System.String s)
		{
			int adjustment = s.Length - (c_ket - c_bra);
            if (current.Length > bra)
    			current.Replace(current.ToString(bra, ket - bra), s, bra, ket - bra);
            else
                current.Append(s);
			limit += adjustment;
			if (cursor >= c_ket)
				cursor += adjustment;
			else if (cursor > c_bra)
				cursor = c_bra;
			return adjustment;
		}
		
		protected internal virtual void  slice_check()
		{
			if (bra < 0 || bra > ket || ket > limit || limit > current.Length)
			// this line could be removed
			{
				System.Console.Error.WriteLine("faulty slice operation");
				// FIXME: report error somehow.
				/*
				fprintf(stderr, "faulty slice operation:\n");
				debug(z, -1, 0);
				exit(1);
				*/
			}
		}
		
		protected internal virtual void  slice_from(System.String s)
		{
			slice_check();
			replace_s(bra, ket, s);
		}
		
		protected internal virtual void  slice_from(System.Text.StringBuilder s)
		{
			slice_from(s.ToString());
		}
		
		protected internal virtual void  slice_del()
		{
			slice_from("");
		}
		
		protected internal virtual void  insert(int c_bra, int c_ket, System.String s)
		{
			int adjustment = replace_s(c_bra, c_ket, s);
			if (c_bra <= bra)
				bra += adjustment;
			if (c_bra <= ket)
				ket += adjustment;
		}
		
		protected internal virtual void  insert(int c_bra, int c_ket, System.Text.StringBuilder s)
		{
			insert(c_bra, c_ket, s.ToString());
		}
		
		/* Copy the slice into the supplied StringBuffer */
		protected internal virtual System.Text.StringBuilder slice_to(System.Text.StringBuilder s)
		{
			slice_check();
			int len = ket - bra;
			//// s.Replace(s.ToString(0, s.Length - 0), current.ToString(bra, ket), 0, s.Length - 0);
			s.Remove(0, s.Length);
            s.Append(current.ToString(bra, len));
			return s;
		}
		
		protected internal virtual System.Text.StringBuilder assign_to(System.Text.StringBuilder s)
		{
			//// s.Replace(s.ToString(0, s.Length - 0), current.ToString(0, limit), 0, s.Length - 0);
			s.Remove(0, s.Length);
            s.Append(current.ToString(0, limit));
			return s;
		}
		
		/*
		extern void debug(struct SN_env * z, int number, int line_count)
		{   int i;
		int limit = SIZE(z->p);
		//if (number >= 0) printf("%3d (line %4d): '", number, line_count);
		if (number >= 0) printf("%3d (line %4d): [%d]'", number, line_count,limit);
		for (i = 0; i <= limit; i++)
		{   if (z->lb == i) printf("{");
		if (z->bra == i) printf("[");
		if (z->c == i) printf("|");
		if (z->ket == i) printf("]");
		if (z->l == i) printf("}");
		if (i < limit)
		{   int ch = z->p[i];
		if (ch == 0) ch = '#';
		printf("%c", ch);
		}
		}
		printf("'\n");
		}*/
	}
	
}