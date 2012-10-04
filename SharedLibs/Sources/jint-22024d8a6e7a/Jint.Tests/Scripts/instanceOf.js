var func = function() {
}

function Foo() {
}

var bar = new Foo();

assert(false, func instanceof String ); // function isn't a string
assert(true, bar instanceof Foo ); // function
assert(true, [1,2,3] instanceof Array ); // array is an array
assert(true, [1,2,3] instanceof Object ); // array is an object