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

using NUnit.Framework;

using LocalizedTestCase = Lucene.Net.Util.LocalizedTestCase;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Documents
{
	
    [TestFixture]
	public class TestDateTools:LocalizedTestCase
	{
		
        [Test]
		public virtual void  TestStringToDate()
		{
			
			System.DateTime d;
			d = DateTools.StringToDate("2004");
			Assert.AreEqual("2004-01-01 00:00:00:000", IsoFormat(d));
			d = DateTools.StringToDate("20040705");
			Assert.AreEqual("2004-07-05 00:00:00:000", IsoFormat(d));
			d = DateTools.StringToDate("200407050910");
			Assert.AreEqual("2004-07-05 09:10:00:000", IsoFormat(d));
			d = DateTools.StringToDate("20040705091055990");
			Assert.AreEqual("2004-07-05 09:10:55:990", IsoFormat(d));
			
			try
			{
				d = DateTools.StringToDate("97"); // no date
				Assert.Fail();
			}
			catch (System.FormatException e)
			{
				/* expected exception */
			}
			try
			{
				d = DateTools.StringToDate("200401011235009999"); // no date
				Assert.Fail();
			}
			catch (System.FormatException e)
			{
				/* expected exception */
			}
			try
			{
				d = DateTools.StringToDate("aaaa"); // no date
				Assert.Fail();
			}
			catch (System.FormatException e)
			{
				/* expected exception */
			}
		}
		
        [Test]
		public virtual void  TestStringtoTime()
		{
			long time = DateTools.StringToTime("197001010000");

			System.DateTime cal = new System.DateTime(1970, 1, 1, 0, 0, 0, 0, new System.Globalization.GregorianCalendar());  // hour, minute, second
			Assert.AreEqual(cal.Ticks, time);

			cal = new System.DateTime(1980, 2, 2, 11, 5, 0, 0, new System.Globalization.GregorianCalendar()); // hour, minute, second
			time = DateTools.StringToTime("198002021105");
			Assert.AreEqual(cal.Ticks, time);
		}
		
        [Test]
		public virtual void  TestDateAndTimetoString()
		{
			System.DateTime cal = new System.DateTime(2004, 2, 3, 22, 8, 56, 333, new System.Globalization.GregorianCalendar());
			
			System.String dateString;
			System.DateTime tempAux = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux, DateTools.Resolution.YEAR);
			Assert.AreEqual("2004", dateString);
			System.DateTime tempAux2 = DateTools.StringToDate(dateString);
			Assert.AreEqual("2004-01-01 00:00:00:000", IsoFormat(tempAux2));
			
			System.DateTime tempAux3 = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux3, DateTools.Resolution.MONTH);
			Assert.AreEqual("200402", dateString);
			System.DateTime tempAux4 = DateTools.StringToDate(dateString);
			Assert.AreEqual("2004-02-01 00:00:00:000", IsoFormat(tempAux4));
			
			System.DateTime tempAux5 = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux5, DateTools.Resolution.DAY);
			Assert.AreEqual("20040203", dateString);
			System.DateTime tempAux6 = DateTools.StringToDate(dateString);
			Assert.AreEqual("2004-02-03 00:00:00:000", IsoFormat(tempAux6));
			
			System.DateTime tempAux7 = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux7, DateTools.Resolution.HOUR);
			Assert.AreEqual("2004020322", dateString);
			System.DateTime tempAux8 = DateTools.StringToDate(dateString);
			Assert.AreEqual("2004-02-03 22:00:00:000", IsoFormat(tempAux8));
			
			System.DateTime tempAux9 = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux9, DateTools.Resolution.MINUTE);
			Assert.AreEqual("200402032208", dateString);
			System.DateTime tempAux10 = DateTools.StringToDate(dateString);
			Assert.AreEqual("2004-02-03 22:08:00:000", IsoFormat(tempAux10));
			
			System.DateTime tempAux11 = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux11, DateTools.Resolution.SECOND);
			Assert.AreEqual("20040203220856", dateString);
			System.DateTime tempAux12 = DateTools.StringToDate(dateString);
			Assert.AreEqual("2004-02-03 22:08:56:000", IsoFormat(tempAux12));
			
			System.DateTime tempAux13 = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux13, DateTools.Resolution.MILLISECOND);
			Assert.AreEqual("20040203220856333", dateString);
			System.DateTime tempAux14 = DateTools.StringToDate(dateString);
			Assert.AreEqual("2004-02-03 22:08:56:333", IsoFormat(tempAux14));
			
			// date before 1970:
			cal = new System.DateTime(1961, 3, 5, 23, 9, 51, 444, new System.Globalization.GregorianCalendar());
			System.DateTime tempAux15 = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux15, DateTools.Resolution.MILLISECOND);
			Assert.AreEqual("19610305230951444", dateString);
			System.DateTime tempAux16 = DateTools.StringToDate(dateString);
			Assert.AreEqual("1961-03-05 23:09:51:444", IsoFormat(tempAux16));
			
			System.DateTime tempAux17 = new System.DateTime(cal.Ticks);
			dateString = DateTools.DateToString(tempAux17, DateTools.Resolution.HOUR);
			Assert.AreEqual("1961030523", dateString);
			System.DateTime tempAux18 = DateTools.StringToDate(dateString);
			Assert.AreEqual("1961-03-05 23:00:00:000", IsoFormat(tempAux18));
			
			// timeToString:
			cal = new System.DateTime(1970, 1, 1, 0, 0, 0, 0, new System.Globalization.GregorianCalendar());
			dateString = DateTools.TimeToString(cal.Ticks / TimeSpan.TicksPerMillisecond, DateTools.Resolution.MILLISECOND);
			Assert.AreEqual("19700101000000000", dateString);
			
			cal = new System.DateTime(1970, 1, 1, 1, 2, 3, 0, new System.Globalization.GregorianCalendar());
			dateString = DateTools.TimeToString(cal.Ticks / TimeSpan.TicksPerMillisecond, DateTools.Resolution.MILLISECOND);
			Assert.AreEqual("19700101010203000", dateString);
		}
		
        [Test]
		public virtual void  TestRound()
		{
			System.DateTime date = new System.DateTime(2004, 2, 3, 22, 8, 56, 333, new System.Globalization.GregorianCalendar());
			Assert.AreEqual("2004-02-03 22:08:56:333", IsoFormat(date));
			
			System.DateTime dateYear = DateTools.Round(date, DateTools.Resolution.YEAR);
			Assert.AreEqual("2004-01-01 00:00:00:000", IsoFormat(dateYear));
			
			System.DateTime dateMonth = DateTools.Round(date, DateTools.Resolution.MONTH);
			Assert.AreEqual("2004-02-01 00:00:00:000", IsoFormat(dateMonth));
			
			System.DateTime dateDay = DateTools.Round(date, DateTools.Resolution.DAY);
			Assert.AreEqual("2004-02-03 00:00:00:000", IsoFormat(dateDay));
			
			System.DateTime dateHour = DateTools.Round(date, DateTools.Resolution.HOUR);
			Assert.AreEqual("2004-02-03 22:00:00:000", IsoFormat(dateHour));
			
			System.DateTime dateMinute = DateTools.Round(date, DateTools.Resolution.MINUTE);
			Assert.AreEqual("2004-02-03 22:08:00:000", IsoFormat(dateMinute));
			
			System.DateTime dateSecond = DateTools.Round(date, DateTools.Resolution.SECOND);
			Assert.AreEqual("2004-02-03 22:08:56:000", IsoFormat(dateSecond));
			
			System.DateTime dateMillisecond = DateTools.Round(date, DateTools.Resolution.MILLISECOND);
			Assert.AreEqual("2004-02-03 22:08:56:333", IsoFormat(dateMillisecond));
			
			// long parameter:
			long dateYearLong = DateTools.Round(date.Ticks / TimeSpan.TicksPerMillisecond, DateTools.Resolution.YEAR);
			System.DateTime tempAux = new System.DateTime(dateYearLong);
			Assert.AreEqual("2004-01-01 00:00:00:000", IsoFormat(tempAux));
			
			long dateMillisecondLong = DateTools.Round(date.Ticks / TimeSpan.TicksPerMillisecond, DateTools.Resolution.MILLISECOND);
			System.DateTime tempAux2 = new System.DateTime(dateMillisecondLong);
			Assert.AreEqual("2004-02-03 22:08:56:333", IsoFormat(tempAux2));
		}
		
		private System.String IsoFormat(System.DateTime date)
		{
            return date.ToString("yyyy-MM-dd HH:mm:ss:fff");
        }
		
        [Test]
		public virtual void  TestDateToolsUTC()
		{
			// Sun, 30 Oct 2005 00:00:00 +0000 -- the last second of 2005's DST in Europe/London
			//long time = 1130630400;
			DateTime time1 = new DateTime(2005, 10, 30);
			DateTime time2 = time1.AddHours(1);
			try
			{
				//TimeZone.setDefault(TimeZone.getTimeZone("Europe/London")); // {{Aroush-2.0}} need porting 'java.util.TimeZone.getTimeZone'
				System.DateTime tempAux = time1;
				System.String d1 = DateTools.DateToString(tempAux, DateTools.Resolution.MINUTE);
				System.DateTime tempAux2 = time2;
				System.String d2 = DateTools.DateToString(tempAux2, DateTools.Resolution.MINUTE);
				Assert.IsFalse(d1.Equals(d2), "different times");
				Assert.AreEqual(DateTools.StringToTime(d1), time1.Ticks, "midnight");
				Assert.AreEqual(DateTools.StringToTime(d2), time2.Ticks, "later");
			}
			finally
			{
				//TimeZone.SetDefault(null);    // {{Aroush-2.0}} need porting 'java.util.TimeZone.setDefault'
			}
		}
	}
}