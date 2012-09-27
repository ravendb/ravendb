var func = new Function('x', 'return x * x;');
var r = func(3);
assert(9, r);
function sayHi(x) {
    alert('Hi, ' + x + '!');
}

sayHi.text = 'Hello World!';
sayHi['text2'] = 'Hello World... again.';

assert('Hello World!', sayHi['text']);
assert('Hello World... again.', sayHi.text2); 
// assign an anonymous function to a variable
var greet = function(x) {
    return 'Hello, ' + x;
};

assert('Hello, MSDN readers', greet('MSDN readers'));

// passing a function as an argument to another
function square(x) {
    return x * x;
}
function operateOn(num, func) {
    return func(num);
}
// displays 256
assert(256, operateOn(16, square));

// functions as return values
function makeIncrementer() {
    return function(x) { return x + 1; };
}
var inc = makeIncrementer();
// displays 8
assert(8, inc(7));

// functions as object properties
var obj = { 'toString' : function() { return 'This is an object.'; } };
// calls obj.toString()
assert('This is an object.', obj.toString());
assert('This is an object.', obj.toString.apply());

// functions stored as array elements
var arr = [];
arr[0] = function(x) { return x * x; };
arr[1] = arr[0](2);
arr[2] = arr[0](arr[1]);
arr[3] = arr[0](arr[2]);

// displays 256
assert(256, arr[3]);
var myDog = {
    'name' : 'Spot',
    'bark' : function() { return 'Woof!'; },
    'displayFullName' : function() {
        ; return this.name + ' The Alpha Dog';
    },
    'chaseMrPostman' : function() { 
        // implementation beyond the scope of this article 
    }    
};
assert('Spot The Alpha Dog', myDog.displayFullName()); 
assert('Woof!', myDog.bark()); // Woof!
assert('Woof!', myDog["bark"]()); // same as previous, indexer should set CallTarget


var f=Function.apply(null,["x", "y", "return x*y;"]);

assert(2, f(1, 2));
assert(4, f(2, 2));

var obj = { 
    a: function(arg1, isThis) {
        if (arg1 == this)
            istrue(isThis);
        else
            isfalse(isThis);
    } 
}

obj.a(obj, true);
obj.a(this, false);

obj.b = function(arg1, isThis) {
    if (arg1 == this)
        istrue(isThis);
    else
        isfalse(isThis);
}
obj.b(obj, true);
obj.b(this, false);

// recursive functions should not conflict their local variables

function fact(n) {
    var i;
    i = n;
    var res = n == 0 ? 1 : n * fact(n - 1);
    assert(n, i);
    return res;
}

fact(3);

var Type = Function;
Type.prototype.test = function() { print('hello'); };
var MyType = function() { };
MyType.prototype = {};
MyType.test();

var foo = 'foo';
function bar() {
    foo = 'bar';    
    var foo;
}

bar();
assert(foo,'foo');
assert(false,bar==null);

// not specified parameters should be 'undefined'
var dummy = function (x, y) {
    return y;
}
assert(2, dummy(1, 2));
assert(undefined, dummy(1));

// prototype functions with undefined parameters should return 'undefined'
Date.prototype.formatDate = function (input, time) {
    return time;
}
var date = new Date("1/1/2007 1:11:11");

assert(2, date.formatDate("Y-m-d", 2));
assert(undefined, date.formatDate("Y-m-d"));

// a function without a return statement should return undefined

var foo = {
    f1 : function() { assert(true, true); },
    f2 : function() { assert(true, true); return 2; }
}

assert(true, foo.f2.call({}) === 2);
assert(true, foo.f2() === 2);
assert(true, foo.f2.apply({}, []) === 2);

assert(true, foo.f1.call({}) === undefined);
assert(true, foo.f1() === undefined);
assert(true, foo.f1.apply({}, []) === undefined);

// bind
var module1 = {  
    owner: "module1",
    getOwner: function() { return this.owner; }  
};  
var module2 = {
	owner: "module2"
};
  
assert("module1", module1.getOwner());
  
module2.getOwner = module1.getOwner;  
assert("module2", module2.getOwner());

var boundGetOwner = module1.getOwner.bind(module2);  
assert("module2", boundGetOwner());

// bind with argument
var module3 = {  
    owner: "module3",
    getOwner: function(arg1, arg2) { return this.owner + ": " + arg1 + ", " + arg2; }  
};  
var module4 = {
	owner: "module4"
};

var boundGetOwner = module3.getOwner.bind(module4, "argument1");  
assert("module4: argument1, argument2", boundGetOwner("argument2"));