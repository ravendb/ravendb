/\\\/Date\((-?[0-9]+)\)\\\//g

//var a = "ab".split(/a*?/); // ["a","b"]
//assert(2, a.length);
//assert("a", a[0]);
//assert("b", a[1]);

//var b = "ab".split(/a*/); // ["","b"]
//assert(2, b.length);
//assert("", b[0]);
//assert("b", b[1]);

var c = "a b c".split(' '); // ["a","b","c"]
assert(3, c.length);
assert("a", c[0]);
assert("b", c[1]);
assert("c", c[2]);

var c = "a b c".split(/\s/); // ["a","b","c"]
assert(3, c.length);
assert("a", c[0]);
assert("b", c[1]);
assert("c", c[2]);

assert('bc', 'abcd'.substring(1, 3));
assert('cd', 'abcd'.substring(2));
assert('abcd', 'abcd'.substring());

assert(0, ''.length);
assert(1, 'a'.length);
assert(4, 'abcd'.length);

assert('a', 'abcd'[0]);
assert('d', 'abcd'[3]);

assert('2a11b', 1 + 1 + 'a' + 1 + 1 + 'b');

assert('©', '\xA9'); // Latin-1 Hex encoding
assert('©', '\251'); // Latin-1 Octal encoding

assert(65, 'ABC'.charCodeAt(0));
assert(66, 'ABC'.charCodeAt(1));
assert(67, 'ABC'.charCodeAt(2));
assert(NaN, 'ABC'.charCodeAt(3));

assert(55296, '\uD800\uDBFF'.charCodeAt(0));
assert(56319, '\uD800\uDBFF'.charCodeAt(1));

assert(1, '1' - '0');
assert('number', typeof ('1' - '0'));

var value = 12345;
value = String(12345);
assert('a2345', value.replace(/1/, 'a'));

assert(3, "canal".lastIndexOf("a"));
assert(1, "canal".lastIndexOf("a", 2));
assert(-1, "canal".lastIndexOf("a", 0));
assert(-1, "canal".lastIndexOf("x"));

assert("morning is upon u", "The morning is upon us.".slice(4, -2));
assert("morning is upon us.", "The morning is upon us.".slice(4));
assert(".", "The morning is upon us.".slice(-1));

assert(-1, 'a'.localeCompare('b'));
assert(1, 'b'.localeCompare('a'));
assert(0, 'a'.localeCompare('a'));

var s1 = "Oh "
var s2 = "what a beautiful "
var s3 = "mornin'."
var s4 = s1.concat(s2, s3);
assert("Oh what a beautiful mornin'.", s4);

assert(0, "Blue Whale".indexOf("Blue"));
assert(-1, "Blue Whale".indexOf("Blute"));
assert(5, "Blue Whale".indexOf("Whale", 0));
assert(5, "Blue Whale".indexOf("Whale", 5));
assert(9, "Blue Whale".indexOf("", 9));
assert(10, "Blue Whale".indexOf("", 10));
assert(10, "Blue Whale".indexOf("", 11));

// String.match tests
assert(true, /^\w+/.test("gib melson"));
assert(false, /^\w+/.test("#$$%#"));
assert('hello', 'hello world'.match('hello').toString());

assert('undefined', undefined + '');

var name = "over there";
assert(5, name.search("there"));
assert(0, name.search("over"));
assert(-1, name.search("should return -1"));

assert('HELLO', String.fromCharCode(72,69,76,76,79));

assert('bc', 'abcd'['substring'](1, 3));
assert('NaN', NaN + '');

// properties on literals should not be remembered
var aname = "a name";
aname.surname = "a surname";
assert(undefined, aname.surname);