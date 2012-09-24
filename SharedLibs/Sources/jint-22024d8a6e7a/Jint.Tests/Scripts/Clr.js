// comparing clr and js

// we can't compare a date time objects for an equality
assert(false, new Date(0) == new System.DateTime(1970, 1, 1, 0, 0, 0, System.DateTimeKind.Utc));

assert(true, new Date(0) < new System.DateTime(1971, 1, 1, 0, 0, 0, System.DateTimeKind.Utc));
assert(true, new Date(0) > new System.DateTime(1969, 1, 1, 0, 0, 0, System.DateTimeKind.Utc));

assert(true, 100 == System.Convert.ToInt32(100));
assert(true, 100 < System.Convert.ToInt32(200));
assert(true, 100 > System.Convert.ToInt32(10));

var d = System.DateTime.Now;
// marshalled datetime objects should be an instance of Date class
assert(true, System.DateTime.Now instanceof Date);
// constructed datetime objects should be an instance of Date class
assert(true, new System.DateTime(1969, 1, 1, 0, 0, 0, System.DateTimeKind.Utc) instanceof Date);

var a = new System.Collections.Generic.List{System.Int32}(); 
a.Add(7); a.Add(3); a.Add(4);

var d = System.DateTime.Now;
assert(System.Convert.ToInt32(new Date().getFullYear()), d.Year);

// implicit constructor call
var d = new System.DateTime(1, 1, 1);
assert(System.Convert.ToInt32(1), d.Year);

// explicit constructor call
var d = new System.DateTime(System.Convert.ToInt32(1), System.Convert.ToInt32(1), System.Convert.ToInt32(1));
assert(System.Convert.ToInt32(1), d.Year);

assert(System.Convert.ToInt32(1), System.Convert.ToInt32(1));

var i = System.Convert.ToInt32(1);

var d = System.DateTime.Now;
assert(System.Convert.ToInt32(new Date().getFullYear()), d.Year);

var dateTime = System.DateTime;
var d = dateTime.Now;
assert(System.Convert.ToInt32(new Date().getFullYear()), d.Year);

// Collections
var a = new System.Collections.Generic.List{System.Int32}(); 
a.Add(7); a.Add(3); a.Add(4);
a.Sort();
a.ForEach(function(i){print(i.toString());});
assert(System.Convert.ToInt32(3), a[0]);
assert(System.Convert.ToInt32(4), a[1]);
assert(System.Convert.ToInt32(7), a[2]);

var map = new System.Collections.Generic.Dictionary{System.String,System.String}();
map["cat"] = "Tom";

var catName;
assert(true,map.TryGetValue("cat",catName));
assert("Tom",catName);

// IEnumerable

var sum = 0;
for(var i in a)
{
    sum += i;
}

assert(14, sum);

// Delegates
var list = new System.Collections.Generic.List{System.Int32}(); 
list.Add(7); list.Add(3); list.Add(4);
list = list.ConvertAll{System.Double}( function (x) { return x * x; });
assert(3, Number(list.Count));
assert(49, list[0]);
assert(9, list[1]);
assert(16, list[2]);


var Hashtable=System.Collections.Hashtable;
var hashtable=new Hashtable();
hashtable.Add(1,"one");
hashtable.Add(2,"two");
hashtable.Add(3,"three");
assert(3, Number(hashtable.Count));

// compare
assert(false, Hashtable == null);
assert(true, Hashtable != null);
assert(false, hashtable == null);
assert(true, hashtable != null);
assert(false, hashtable.Add == null);
assert(true, hashtable.Add != null);

// IComparable tests
var ts1 = new System.TimeSpan(1000);
var ts3 = new System.TimeSpan(1000);
var ts2 = new System.TimeSpan(2000);

assert(true, ts1 < ts2);
assert(true, ts1 <= ts2);
assert(false, ts1 > ts2);
assert(false, ts1 >= ts2);
assert(false, ts1 == ts2);

assert(true, ts1 == ts3);
assert(true, ts1 >= ts3);
assert(true, ts1 <= ts3);
assert(false, ts1 > ts3);
assert(false, ts1 < ts3);
