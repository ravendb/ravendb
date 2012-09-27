//Suppose you define the following variables:

var myFun = new Function("return 5+2;");
var shape = "round";
var size = 1;
var today = new Date();

// The typeof operator returns the following results for these variables:

assert('function', typeof myFun);
assert('string', typeof shape);
assert('number', typeof size);
assert('object', typeof today);
assert('undefined', typeof dontExist);
// For the keywords true and null, the typeof operator returns the following results:

assert('boolean', typeof true);
assert('object', typeof null);

// For a number or string, the typeof operator returns the following results:

assert('number', typeof 62);
assert('string', typeof 'Hello world');

//For property values, the typeof operator returns the type of value the property contains:

assert('number', typeof Math.LN2);

// For methods and functions, the typeof operator returns results as follows:

assert('function', typeof eval);

//For predefined objects, the typeof operator returns results as follows:

assert('function', typeof Date);
assert('function', typeof Function);
assert('object', typeof Math);
assert('function', typeof Object);
assert('function', typeof String);

assert('boolean', typeof true);
assert('object', typeof new Boolean(true));
assert('boolean', typeof Boolean(true));
assert('object', typeof new Number(0));
assert('number', typeof Number(0));
assert('function', typeof new Function());
assert('object', typeof ['foo']);

assert('[object Array]', Object.prototype.toString.apply([]));
assert('[object Boolean]', Object.prototype.toString.apply(true));
assert('[object Date]', Object.prototype.toString.apply(new Date()));
assert('[object Error]', Object.prototype.toString.apply(new Error()));
assert('[object Function]', Object.prototype.toString.apply(function f() { }));
assert('[object Number]', Object.prototype.toString.apply(1));
assert('[object Object]', Object.prototype.toString.apply(new Object()));
assert('[object RegExp]', Object.prototype.toString.apply(/x/));
assert('[object String]', Object.prototype.toString.apply('foo'));

var opts = Object.prototype.toString;
assert('[object Undefined]',opts.call(undefined));
assert('[object Global]',opts());
